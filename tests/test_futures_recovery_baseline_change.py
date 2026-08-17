from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

import grid_optimizer.futures_recovery_coordinator as recovery
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_KEY,
    RECOVERY_STATE_MIRROR_KEY,
    JsonRecoveryStore,
    RecoveryStateCorruptError,
)
from grid_optimizer import web as web_module


NOW = datetime(2026, 8, 17, 3, 0, tzinfo=timezone.utc)
BASELINE = {
    "symbol": "ARXUSDT",
    "strategy_profile": "volume_long_v4",
    "strategy_mode": "one_way_long",
    "step_price": 0.0005,
    "best_quote_maker_volume_cycle_budget_notional": 360.0,
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}
NEW_BASELINE = {
    **BASELINE,
    "step_price": 0.0007,
    "best_quote_maker_volume_cycle_budget_notional": 420.0,
    # Protected values are intentionally hostile; materialization must own them.
    "best_quote_maker_volume_allow_loss_reduce_only": True,
    "best_quote_maker_volume_net_loss_reduce_enabled": True,
    "hard_loss_forced_reduce_enabled": True,
    "volatility_entry_pause_enabled": False,
}


def _snapshot(
    symbol: str,
    now: datetime,
    state: recovery.RecoveryState,
    *,
    assessment: recovery.FlowBlockerAssessment | None = None,
    effect_receipt: recovery.EffectReceipt | None = None,
) -> recovery.SymbolSnapshot:
    return recovery.SymbolSnapshot(
        symbol=symbol,
        captured_at=now,
        assessment=assessment or recovery.FlowBlockerAssessment(),
        effect_receipt=effect_receipt,
    )


def _request(
    *,
    operation_id: str = "baseline-op-1",
    attempt_id: str = "attempt-1",
    candidate: dict[str, object] | None = None,
    requested_at: datetime = NOW,
) -> object:
    return recovery.BaselineChange.create(
        symbol="ARXUSDT",
        operation_id=operation_id,
        attempt_id=attempt_id,
        source="test",
        requested_at=requested_at,
        candidate_baseline=candidate or NEW_BASELINE,
    )


def _coordinator(
    store: JsonRecoveryStore,
    *,
    assessment: recovery.FlowBlockerAssessment | None = None,
    effects: list[recovery.EffectCommand] | None = None,
) -> recovery.FuturesRecoveryCoordinator:
    sink = effects if effects is not None else []
    return recovery.FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=lambda symbol, now, state: _snapshot(
            symbol,
            now,
            state,
            assessment=assessment,
        ),
        effect_executor=lambda _symbol, command: sink.append(command),
    )


def _register(path: Path) -> JsonRecoveryStore:
    path.write_text(json.dumps({"symbol": "ARXUSDT"}), encoding="utf-8")
    store = JsonRecoveryStore(path)
    store.register_symbol("ARXUSDT", BASELINE, now=NOW)
    return store


def _make_active(store: JsonRecoveryStore) -> recovery.RecoveryState:
    state = store.read("ARXUSDT")
    entered = recovery.FuturesRecoveryDecisionEngine().plan_round(
        snapshot=_snapshot(
            "ARXUSDT",
            NOW,
            state,
            assessment=recovery.FlowBlockerAssessment(
                inventory_reduce_sides=(recovery.Side.SELL,)
            ),
        ),
        state=state,
        now=NOW,
        round_id="existing-runner-recovery",
    )
    store.compare_and_swap(
        "ARXUSDT",
        expected_revision=state.document_revision,
        next_state=entered.next_state,
    )
    return entered.next_state


def test_web_active_baseline_change_is_durable_across_restart(tmp_path: Path) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    active = _make_active(store)
    before_profile = active.desired_profile

    with patch.object(
        web_module,
        "_runner_control_path",
        return_value=control_path,
    ), patch.object(web_module, "_save_runner_control_config") as save_control, patch.object(
        web_module, "_clear_volatility_trigger_status"
    ) as clear_trigger, patch.object(
        web_module, "_read_runner_process_for_symbol"
    ) as read_runner:
        result = web_module._save_runner_config_without_start(
            {
                "symbol": "ARXUSDT",
                "operation_id": "web-baseline-op",
                "attempt_id": "web-attempt-1",
                "source": "running_status_web",
                "step_price": 0.0007,
            }
        )

    assert result["request_status"] == "deferred"
    assert result["operation_id"] == "web-baseline-op"
    assert result["attempt_id"] == "web-attempt-1"
    save_control.assert_not_called()
    clear_trigger.assert_not_called()
    read_runner.assert_not_called()

    restarted_store = JsonRecoveryStore(control_path)
    restarted_state = restarted_store.read("ARXUSDT")
    assert restarted_state.phase is recovery.RecoveryPhase.ACTIVE
    assert restarted_state.active_action is recovery.ActionId.INVENTORY_RECOVER
    assert restarted_state.desired_profile == before_profile
    assert restarted_state.baseline_change is not None
    assert restarted_state.baseline_change.request.operation_id == "web-baseline-op"
    assert (
        "recovery_order_side"
        not in restarted_state.baseline_change.request.candidate_profile.fields
    )
    assert (
        restarted_state.baseline_change.request.candidate_profile.fields[
            "best_quote_maker_volume_cycle_budget_notional"
        ]
        == 360.0
    )
    assert (
        restarted_state.baseline_change.status
        is recovery.BaselineChangeStatus.DEFERRED
    )


def test_monitor_editor_round_trip_never_captures_registered_control_overlay(
    tmp_path: Path,
) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    _make_active(store)
    monitor_payload = json.loads(control_path.read_text(encoding="utf-8"))
    assert RECOVERY_STATE_KEY in monitor_payload
    assert RECOVERY_STATE_MIRROR_KEY in monitor_payload
    assert monitor_payload["recovery_order_side"] == "SELL"
    monitor_payload.update(
        {
            "operation_id": "monitor-round-trip-op",
            "attempt_id": "monitor-round-trip-attempt",
            "source": "running_status_web",
            "requested_at": NOW.isoformat(),
            "step_price": 0.0008,
            "autotune_symbol_enabled": False,
            # Coordinator-owned fields shown by the monitor are never edits
            # to the durable baseline, even if the browser posts them back.
            "best_quote_maker_volume_allow_loss_reduce_only": True,
            "volatility_entry_pause_enabled": False,
        }
    )

    with patch.object(
        web_module,
        "_runner_control_path",
        return_value=control_path,
    ):
        submitted = web_module._save_runner_config_without_start(monitor_payload)

    assert submitted["request_status"] == "deferred"
    deferred_state = JsonRecoveryStore(control_path).read("ARXUSDT")
    assert deferred_state.baseline_change is not None
    candidate = deferred_state.baseline_change.request.candidate_profile.fields
    assert candidate["step_price"] == 0.0008
    for key in (RECOVERY_STATE_KEY, RECOVERY_STATE_MIRROR_KEY):
        assert key not in candidate
    assert "recovery_order_side" not in candidate
    assert "near_market_entry_max_center_distance_steps" not in candidate
    assert {
        key for key in candidate if web_module._is_managed_recovery_key(key)
    } == {
        "best_quote_maker_volume_allow_loss_reduce_only",
        "best_quote_maker_volume_net_loss_reduce_enabled",
        "hard_loss_forced_reduce_enabled",
        "volatility_entry_pause_enabled",
    }
    for key in ("operation_id", "attempt_id", "source", "requested_at"):
        assert key not in candidate

    # Model the existing higher-priority recovery completing normally.  The
    # next strict-STABLE coordinator round owns applying the durable request.
    reached_stable = replace(
        recovery.RecoveryState.initial(
            "ARXUSDT",
            deferred_state.baseline_profile.fields,
            now=NOW + timedelta(seconds=1),
        ),
        document_revision=deferred_state.document_revision + 1,
        generation=deferred_state.generation,
        effect_epoch=deferred_state.effect_epoch,
        recent_round_ids=deferred_state.recent_round_ids,
        last_round_id=deferred_state.last_round_id,
        baseline_changes=deferred_state.baseline_changes,
    )
    store.compare_and_swap(
        "ARXUSDT",
        expected_revision=deferred_state.document_revision,
        next_state=reached_stable,
    )

    effects: list[recovery.EffectCommand] = []
    coordinator = _coordinator(JsonRecoveryStore(control_path), effects=effects)
    applied = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=2),
        round_id="apply-monitor-round-trip",
    )
    applied_state = JsonRecoveryStore(control_path).read("ARXUSDT")
    assert applied.action_id is recovery.ActionId.BASELINE_REBASE
    assert applied.effect_stage is recovery.EffectStage.RUNNER_RESTART
    assert len(effects) == 1
    assert applied_state.baseline_profile.fields["step_price"] == 0.0008
    assert applied_state.baseline_profile == applied_state.desired_profile
    for profile in (applied_state.baseline_profile, applied_state.desired_profile):
        for key in (RECOVERY_STATE_KEY, RECOVERY_STATE_MIRROR_KEY):
            assert key not in profile.fields
        assert "recovery_order_side" not in profile.fields
        assert (
            profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
            is False
        )
        assert (
            profile.fields["best_quote_maker_volume_net_loss_reduce_enabled"]
            is False
        )
        assert profile.fields["hard_loss_forced_reduce_enabled"] is False
        assert profile.fields["volatility_entry_pause_enabled"] is True
        assert profile.execution_policy == recovery.ExecutionPolicy(
            order_type="LIMIT",
            time_in_force="GTX",
            post_only=True,
        )

    receipt = recovery.EffectReceipt(
        decision_id=str(applied_state.decision_id),
        stage=recovery.EffectStage.RUNNER_RESTART,
        effect_epoch=int(applied.effect_epoch),
        observed_at=NOW + timedelta(seconds=3),
    )
    coordinator.snapshot_provider = lambda symbol, now, state: _snapshot(
        symbol,
        now,
        state,
        effect_receipt=receipt,
    )
    settled = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=3),
        round_id="receipt-monitor-round-trip",
    )
    assert settled.next_state.phase is recovery.RecoveryPhase.STABLE

    fresh_store = JsonRecoveryStore(control_path)
    fresh_state = fresh_store.read("ARXUSDT")
    post_receipt = _coordinator(fresh_store).reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=4),
        round_id="post-monitor-round-trip",
    )
    assert post_receipt.action_id is recovery.ActionId.NOOP
    assert post_receipt.effect_stage is recovery.EffectStage.NONE
    assert post_receipt.next_state.baseline_profile == fresh_state.baseline_profile


def test_stable_round_applies_baseline_once_and_restarts_once(tmp_path: Path) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    effects: list[recovery.EffectCommand] = []
    coordinator = _coordinator(store, effects=effects)

    submitted = coordinator.change_baseline(_request())
    before_apply = store.read("ARXUSDT")
    assert submitted.status is recovery.BaselineChangeStatus.DEFERRED

    applied = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=1),
        round_id="apply-baseline-op-1",
    )
    applied_state = store.read("ARXUSDT")

    assert applied.action_id is recovery.ActionId.BASELINE_REBASE
    assert applied.effect_stage is recovery.EffectStage.RUNNER_RESTART
    assert len(effects) == 1
    assert applied_state.document_revision == before_apply.document_revision + 1
    assert applied_state.generation == before_apply.generation + 1
    assert applied_state.baseline_change is not None
    assert applied_state.baseline_change.status is recovery.BaselineChangeStatus.APPLIED
    assert applied_state.baseline_change.applied_round_id == "apply-baseline-op-1"
    assert applied_state.baseline_profile == applied_state.desired_profile
    assert applied_state.baseline_profile.fields["step_price"] == 0.0007
    assert (
        applied_state.baseline_profile.fields[
            "best_quote_maker_volume_allow_loss_reduce_only"
        ]
        is False
    )
    assert (
        applied_state.baseline_profile.fields[
            "best_quote_maker_volume_net_loss_reduce_enabled"
        ]
        is False
    )
    assert applied_state.baseline_profile.fields["hard_loss_forced_reduce_enabled"] is False
    assert applied_state.baseline_profile.fields["volatility_entry_pause_enabled"] is True
    assert applied_state.baseline_profile.execution_policy == recovery.ExecutionPolicy(
        order_type="LIMIT",
        time_in_force="GTX",
        post_only=True,
    )

    retry = coordinator.change_baseline(
        _request(attempt_id="attempt-2", requested_at=NOW + timedelta(seconds=2))
    )
    assert retry.status is recovery.BaselineChangeStatus.APPLIED
    assert retry.attempt_id == "attempt-2"
    assert store.read("ARXUSDT") == applied_state
    assert len(effects) == 1

    receipt = recovery.EffectReceipt(
        decision_id=str(applied_state.decision_id),
        stage=recovery.EffectStage.RUNNER_RESTART,
        effect_epoch=int(applied.effect_epoch),
        observed_at=NOW + timedelta(seconds=3),
    )
    coordinator.snapshot_provider = lambda symbol, now, state: _snapshot(
        symbol,
        now,
        state,
        effect_receipt=receipt,
    )
    settled = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=3),
        round_id="ack-baseline-op-1",
    )
    assert settled.next_state.phase is recovery.RecoveryPhase.STABLE
    assert settled.effect_stage is recovery.EffectStage.NONE

    coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=4),
        round_id="post-baseline-op-1",
    )
    assert len(effects) == 1


def test_failed_restart_dispatch_retries_the_same_fenced_effect_after_restart(
    tmp_path: Path,
) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    coordinator = recovery.FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=lambda symbol, now, state: _snapshot(symbol, now, state),
        effect_executor=lambda _symbol, _command: (_ for _ in ()).throw(
            RuntimeError("dispatch failed")
        ),
    )
    coordinator.change_baseline(_request(operation_id="dispatch-retry-op"))

    failed = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=1),
        round_id="dispatch-failed",
    )
    failed_state = store.read("ARXUSDT")
    assert failed.effect_error == "dispatch failed"
    assert failed.effect_epoch is not None

    retried_effects: list[recovery.EffectCommand] = []
    restarted = _coordinator(
        JsonRecoveryStore(control_path),
        effects=retried_effects,
    )
    retry = restarted.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=2),
        round_id="retry-after-process-restart",
    )

    assert retry.effect_stage is recovery.EffectStage.RUNNER_RESTART
    assert retry.effect_epoch == failed.effect_epoch
    assert retry.next_state.generation == failed_state.generation
    assert len(retried_effects) == 1
    assert retried_effects[0].effect_epoch == failed.effect_epoch


def test_same_round_retries_pending_baseline_restart_after_cas_before_effect_crash(
    tmp_path: Path,
) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    round_id = "same-round-cas-before-effect-crash"
    coordinator = recovery.FuturesRecoveryCoordinator(
        store=store,
        snapshot_provider=lambda symbol, now, state: _snapshot(symbol, now, state),
        effect_executor=lambda _symbol, _command: (_ for _ in ()).throw(
            SystemExit("process crashed before effect dispatch")
        ),
    )
    coordinator.change_baseline(_request(operation_id="same-round-crash-op"))

    with pytest.raises(SystemExit, match="before effect dispatch"):
        coordinator.reconcile_symbol(
            "ARXUSDT",
            now=NOW + timedelta(seconds=1),
            round_id=round_id,
        )
    crashed_state = JsonRecoveryStore(control_path).read("ARXUSDT")
    crashed_generation = crashed_state.generation
    crashed_revision = crashed_state.document_revision
    assert crashed_state.decision_id == f"ARXUSDT:{round_id}"
    assert crashed_state.pending_effect_stage is recovery.EffectStage.RUNNER_RESTART
    assert crashed_state.pending_effect_epoch is not None

    retried_effects: list[recovery.EffectCommand] = []
    restarted = _coordinator(
        JsonRecoveryStore(control_path),
        effects=retried_effects,
    )
    replay = restarted.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=2),
        round_id=round_id,
    )

    assert replay.effect_stage is recovery.EffectStage.RUNNER_RESTART
    assert replay.effect_epoch == crashed_state.pending_effect_epoch
    assert replay.next_state.decision_id == crashed_state.decision_id
    assert replay.next_state.generation == crashed_generation
    assert replay.next_state.document_revision == crashed_revision
    assert retried_effects == [
        recovery.EffectCommand(
            decision_id=str(crashed_state.decision_id),
            stage=recovery.EffectStage.RUNNER_RESTART,
            effect_epoch=int(crashed_state.pending_effect_epoch),
        )
    ]

    receipt = recovery.EffectReceipt(
        decision_id=str(crashed_state.decision_id),
        stage=recovery.EffectStage.RUNNER_RESTART,
        effect_epoch=int(crashed_state.pending_effect_epoch),
        observed_at=NOW + timedelta(seconds=3),
    )
    restarted.snapshot_provider = lambda symbol, now, state: _snapshot(
        symbol,
        now,
        state,
        effect_receipt=receipt,
    )
    settled = restarted.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=3),
        round_id="same-round-crash-receipt",
    )

    assert settled.next_state.phase is recovery.RecoveryPhase.STABLE
    assert settled.effect_stage is recovery.EffectStage.NONE
    assert len(retried_effects) == 1


def test_higher_priority_recovery_wins_and_baseline_stays_deferred(
    tmp_path: Path,
) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    coordinator = _coordinator(
        store,
        assessment=recovery.FlowBlockerAssessment(
            runner_faults=("runner_inactive",)
        ),
    )
    original_baseline = store.read("ARXUSDT").baseline_profile
    coordinator.change_baseline(_request())

    plan = coordinator.reconcile_symbol(
        "ARXUSDT",
        now=NOW + timedelta(seconds=1),
        round_id="runner-wins",
    )
    state = store.read("ARXUSDT")

    assert plan.action_id is recovery.ActionId.RUNNER_RECOVER
    assert recovery.ActionId.BASELINE_REBASE in plan.suppressed_actions
    assert state.baseline_profile == original_baseline
    assert state.baseline_change is not None
    assert state.baseline_change.status is recovery.BaselineChangeStatus.DEFERRED


def test_operation_id_is_idempotent_but_cannot_change_payload(tmp_path: Path) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    coordinator = _coordinator(store)

    first = coordinator.change_baseline(_request())
    first_state = store.read("ARXUSDT")
    same = coordinator.change_baseline(
        _request(attempt_id="attempt-2", requested_at=NOW + timedelta(seconds=1))
    )

    assert first.status is same.status is recovery.BaselineChangeStatus.DEFERRED
    assert same.attempt_id == "attempt-2"
    assert store.read("ARXUSDT") == first_state

    with pytest.raises(ValueError, match="operation_id.*different payload"):
        coordinator.change_baseline(
            _request(
                attempt_id="attempt-3",
                candidate={**NEW_BASELINE, "step_price": 0.0009},
            )
        )
    assert store.read("ARXUSDT") == first_state


def test_corrupt_control_is_not_overwritten_by_baseline_change(tmp_path: Path) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    control_path.write_bytes(b"{corrupt recovery control")
    before = control_path.read_bytes()
    coordinator = _coordinator(JsonRecoveryStore(control_path))

    with pytest.raises(RecoveryStateCorruptError):
        coordinator.change_baseline(_request())

    assert control_path.read_bytes() == before


def test_web_corrupt_control_is_byte_preserving(tmp_path: Path) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    control_path.write_bytes(b"{corrupt registered recovery control")
    before = control_path.read_bytes()

    with patch.object(
        web_module,
        "_runner_control_path",
        return_value=control_path,
    ):
        result = web_module._save_runner_config_without_start(
            {"symbol": "ARXUSDT", "step_price": 0.0007}
        )

    assert result["reason"] == "runner_control_unreadable"
    assert control_path.read_bytes() == before


def test_baseline_change_rejects_non_finite_and_cross_symbol_candidates() -> None:
    with pytest.raises(ValueError, match="symbol-bound"):
        recovery.BaselineChange.create(
            symbol="ARXUSDT",
            operation_id="op-cross-symbol",
            attempt_id="attempt-1",
            source="test",
            requested_at=NOW,
            candidate_baseline={**NEW_BASELINE, "symbol": "BCHUSDT"},
        )
    with pytest.raises(ValueError):
        recovery.BaselineChange.create(
            symbol="ARXUSDT",
            operation_id="op-non-finite",
            attempt_id="attempt-1",
            source="test",
            requested_at=NOW,
            candidate_baseline={**NEW_BASELINE, "step_price": float("nan")},
        )


def test_existing_schema_v1_envelope_without_baseline_change_remains_readable(
    tmp_path: Path,
) -> None:
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    store = _register(control_path)
    document = json.loads(control_path.read_text(encoding="utf-8"))
    for slot in (RECOVERY_STATE_KEY, RECOVERY_STATE_MIRROR_KEY):
        document[slot]["state"].pop("baseline_changes", None)
    control_path.write_text(json.dumps(document), encoding="utf-8")

    recovered = JsonRecoveryStore(control_path).read("ARXUSDT")

    assert recovered.baseline_change is None
