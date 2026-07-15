from __future__ import annotations

import json
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType

import pytest

from grid_optimizer.futures_recovery_coordinator import (
    ActionAttempt,
    ActionId,
    ActionLease,
    CleanupObligation,
    EffectStage,
    FlowBlockerAssessment,
    FlowClock,
    FlowRoleKey,
    LedgerClass,
    OrderRole,
    FuturesRecoveryDecisionEngine,
    RecoveryPhase,
    RecoveryState,
    SafetyLease,
    Side,
    SymbolSnapshot,
    materialize_profile,
)
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_KEY,
    JsonRecoveryStore,
    RecoveryStateCorruptError,
    RecoveryStateUnavailableError,
)


NOW = datetime(2026, 7, 15, 2, 3, 4, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_cycle_budget_notional": 360.0,
    "volatility_entry_pause_enabled": True,
    "nested_recovery_policy": {
        "allowed_sides": ["BUY", "SELL"],
        "limits": {"max_orders": 3},
    },
}


def _full_state() -> RecoveryState:
    baseline_profile = materialize_profile(
        baseline=BASELINE,
        action_id=ActionId.NOOP,
    )
    action_lease = ActionLease(
        epoch=7,
        action_id=ActionId.TEMPORARY_LOSS_RELIEF,
        side=Side.SELL,
        order_role=OrderRole.REDUCE_ONLY,
        started_at=NOW - timedelta(seconds=20),
        hard_expires_at=NOW + timedelta(seconds=40),
        activated_at=NOW - timedelta(seconds=19),
    )
    safety_lease = SafetyLease(
        epoch=4,
        started_at=NOW - timedelta(seconds=15),
        hard_expires_at=NOW + timedelta(seconds=30),
        evidence_fingerprint="risk-seq-41",
        observation_seq=41,
        observed_at=NOW - timedelta(seconds=1),
    )
    desired_profile = materialize_profile(
        baseline=BASELINE,
        action_id=ActionId.SAFETY_CONVERGE,
        action_updates={
            "recovery_entry_blocked_sides": ["BUY", "SELL"],
            "recovery_reduce_only_allowed_sides": ["BUY", "SELL"],
        },
        safety_lease=safety_lease,
    )
    flow_key = FlowRoleKey(
        LedgerClass.NORMAL_BQ,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
    )
    return RecoveryState(
        symbol="ARXUSDT",
        document_revision=9,
        generation=5,
        baseline_profile=baseline_profile,
        desired_profile=desired_profile,
        desired_runner_state="running",
        phase=RecoveryPhase.CLEANING,
        active_action=ActionId.SAFETY_CONVERGE,
        decision_id="safety-decision-9",
        side=None,
        order_role=None,
        reasons=("volatility",),
        issued_at=NOW - timedelta(seconds=15),
        progress_deadline_at=NOW + timedelta(seconds=10),
        hard_expires_at=NOW + timedelta(seconds=30),
        cooldown_until=None,
        action_lease=action_lease,
        safety_lease=safety_lease,
        cleanup_obligation=CleanupObligation(
            source_action_id=ActionId.TEMPORARY_LOSS_RELIEF,
            source_decision_id="loss-decision-8",
            action_lease_epoch=7,
            open_order_ids=("order-2", "order-1"),
            needs_manifest_rebuild=True,
            created_at=NOW - timedelta(seconds=12),
            attempt_count=2,
            next_retry_at=NOW + timedelta(seconds=3),
        ),
        exhausted_attempts=(
            ActionAttempt(
                action_id=ActionId.INVENTORY_RECOVER,
                side=Side.SELL,
                order_role=OrderRole.REDUCE_ONLY,
                exhausted_at=NOW - timedelta(minutes=1),
            ),
            ActionAttempt(
                action_id=ActionId.MAKER_FLOW_RECOVER,
                side=Side.BUY,
                order_role=OrderRole.ENTRY,
                exhausted_at=NOW - timedelta(minutes=2),
            ),
        ),
        flow_clocks=MappingProxyType(
            {
                flow_key: FlowClock(
                    quote_missing_since=NOW - timedelta(seconds=31),
                    submit_missing_since=NOW - timedelta(seconds=21),
                    fill_missing_since=NOW - timedelta(seconds=11),
                )
            }
        ),
        action_lease_epoch=7,
        safety_lease_epoch=4,
        temporary_loss_lease_uses=MappingProxyType({"episode-a": 2, "episode-b": 1}),
        active_episode_fingerprint="episode-b",
        cleanup_successor_action=ActionId.SAFETY_CONVERGE,
        post_cleanup_effect_stage=EffectStage.NONE,
        terminal_stop_confirmed=False,
        effect_epoch=13,
        pending_effect_stage=EffectStage.MANAGED_GTX_CANCEL,
        pending_effect_epoch=13,
        recent_round_ids=("round-7", "round-8", "round-9"),
        last_round_id="round-9",
    )


def _active_temporary_loss_state() -> RecoveryState:
    state = _full_state()
    lease = state.action_lease
    profile = materialize_profile(
        baseline=BASELINE,
        action_id=ActionId.TEMPORARY_LOSS_RELIEF,
        action_updates={
            "recovery_order_side": Side.SELL.value,
            "recovery_order_role": OrderRole.REDUCE_ONLY.value,
        },
        action_lease=lease,
        action_lease_active=True,
    )
    return replace(
        state,
        desired_profile=profile,
        phase=RecoveryPhase.ACTIVE,
        active_action=ActionId.TEMPORARY_LOSS_RELIEF,
        decision_id="loss-decision-8",
        side=Side.SELL,
        order_role=OrderRole.REDUCE_ONLY,
        reasons=("soft_inventory_exceeded", "loss_only_blocked"),
        issued_at=NOW - timedelta(seconds=20),
        progress_deadline_at=NOW + timedelta(seconds=10),
        hard_expires_at=NOW + timedelta(seconds=40),
        safety_lease=None,
        cleanup_obligation=None,
        cleanup_successor_action=None,
        post_cleanup_effect_stage=EffectStage.NONE,
        pending_effect_stage=EffectStage.NONE,
        pending_effect_epoch=None,
    )


def _settling_temporary_loss_state() -> RecoveryState:
    state = _active_temporary_loss_state()
    lease = replace(state.action_lease, activated_at=None)
    profile = materialize_profile(
        baseline=BASELINE,
        action_id=ActionId.TEMPORARY_LOSS_RELIEF,
        action_updates={
            "recovery_order_side": Side.SELL.value,
            "recovery_order_role": OrderRole.REDUCE_ONLY.value,
        },
        action_lease=lease,
        action_lease_active=False,
    )
    return replace(
        state,
        desired_profile=profile,
        phase=RecoveryPhase.SETTLING,
        action_lease=lease,
        cleanup_obligation=None,
        cleanup_successor_action=None,
        post_cleanup_effect_stage=EffectStage.NONE,
        terminal_stop_confirmed=False,
        pending_effect_stage=EffectStage.NONE,
        pending_effect_epoch=None,
    )


def _active_safety_state() -> RecoveryState:
    return replace(
        _full_state(),
        phase=RecoveryPhase.ACTIVE,
        action_lease=None,
        cleanup_obligation=None,
        cleanup_successor_action=None,
        pending_effect_stage=EffectStage.NONE,
        pending_effect_epoch=None,
    )


def _restoring_state() -> RecoveryState:
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    return replace(
        state,
        document_revision=3,
        generation=2,
        phase=RecoveryPhase.RESTORING,
        active_action=ActionId.MAKER_FLOW_RECOVER,
        decision_id="maker-decision-2",
        side=Side.BUY,
        order_role=OrderRole.ENTRY,
        reasons=("condition_cleared",),
        issued_at=NOW - timedelta(seconds=20),
        progress_deadline_at=NOW + timedelta(seconds=10),
        hard_expires_at=NOW + timedelta(seconds=40),
        last_round_id="restore-3",
    )


def _cooldown_state() -> RecoveryState:
    return replace(
        _restoring_state(),
        document_revision=4,
        phase=RecoveryPhase.COOLDOWN,
        progress_deadline_at=None,
        cooldown_until=NOW + timedelta(seconds=5),
        last_round_id="cooldown-4",
    )


def _stop_pending_state() -> RecoveryState:
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    return replace(
        state,
        document_revision=1,
        generation=1,
        desired_runner_state="stopped",
        phase=RecoveryPhase.STOP_PENDING,
        active_action=ActionId.TERMINAL_STOP,
        decision_id="terminal-decision-1",
        reasons=("trusted_competition_window_end",),
        issued_at=NOW,
        progress_deadline_at=NOW + timedelta(seconds=10),
        hard_expires_at=NOW + timedelta(seconds=30),
        effect_epoch=1,
        pending_effect_stage=EffectStage.RUNNER_STOP,
        pending_effect_epoch=1,
        last_round_id="terminal-1",
    )


def _stopped_state() -> RecoveryState:
    return replace(
        _stop_pending_state(),
        document_revision=2,
        phase=RecoveryPhase.STOPPED,
        terminal_stop_confirmed=True,
        pending_effect_stage=EffectStage.NONE,
        pending_effect_epoch=None,
        last_round_id="stopped-2",
    )


def _read_document(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_recovery_state_round_trips_every_field_and_deep_profile() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        state = _full_state()
        assert set(JsonRecoveryStore.encode_state(state)) == {
            item.name for item in fields(RecoveryState)
        }
        encoded_state = JsonRecoveryStore.encode_state(state)
        flat_profile = encoded_state["desired_profile"]["fields"]
        control_path.write_text(
            json.dumps(
                {
                    **flat_profile,
                    "manual_unmanaged_switch": {"enabled": True},
                    RECOVERY_STATE_KEY: {
                        "schema_version": 1,
                        "state": encoded_state,
                    },
                }
            ),
            encoding="utf-8",
        )

        loaded = JsonRecoveryStore(control_path).read("ARXUSDT")

        assert loaded == state
        assert loaded.baseline_profile.fields["nested_recovery_policy"][
            "allowed_sides"
        ] == ("BUY", "SELL")
        assert (
            loaded.desired_profile.fields[
                "best_quote_maker_volume_allow_loss_reduce_only"
            ]
            is False
        )
        assert loaded.action_lease is not None
        assert loaded.action_lease.activated_at == NOW - timedelta(seconds=19)
        assert loaded.safety_lease is not None
        assert loaded.safety_lease.observation_seq == 41
        assert loaded.cleanup_obligation is not None
        assert loaded.cleanup_obligation.open_order_ids == ("order-2", "order-1")
        assert tuple(loaded.flow_clocks) == tuple(state.flow_clocks)
        assert loaded.temporary_loss_lease_uses == {
            "episode-a": 2,
            "episode-b": 1,
        }
        assert loaded.cleanup_successor_action is ActionId.SAFETY_CONVERGE
        assert loaded.post_cleanup_effect_stage is EffectStage.NONE
        assert loaded.terminal_stop_confirmed is False
        assert loaded.effect_epoch == 13
        assert loaded.pending_effect_stage is EffectStage.MANAGED_GTX_CANCEL
        assert loaded.pending_effect_epoch == 13


def test_register_uses_only_explicit_baseline_and_preserves_unmanaged_fields() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        control_path.write_text(
            json.dumps(
                {
                    "manual_unmanaged_switch": "keep-me",
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    "volatility_entry_pause_enabled": False,
                }
            ),
            encoding="utf-8",
        )
        store = JsonRecoveryStore(control_path)

        registered = store.register_symbol("arxusdt", BASELINE, now=NOW)

        document = _read_document(control_path)
        assert registered.document_revision == 0
        assert registered.symbol == "ARXUSDT"
        assert document["manual_unmanaged_switch"] == "keep-me"
        assert document["best_quote_maker_volume_allow_loss_reduce_only"] is False
        assert document["volatility_entry_pause_enabled"] is True
        assert document[RECOVERY_STATE_KEY]
        assert store.read("ARXUSDT") == registered
        with pytest.raises(ValueError, match="already registered"):
            JsonRecoveryStore(control_path).register_symbol(
                "ARXUSDT",
                {"best_quote_maker_volume_cycle_budget_notional": 999.0},
                now=NOW + timedelta(days=1),
            )
        assert JsonRecoveryStore(control_path).read("ARXUSDT") == registered


def test_stale_recovery_metadata_is_removed_and_rejected_if_reintroduced() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        control_path.write_text(
            json.dumps(
                {
                    "manual_unmanaged_switch": "keep-me",
                    "recovery_action_lease_active": True,
                    "recovery_action_lease_epoch": 999,
                }
            ),
            encoding="utf-8",
        )
        store = JsonRecoveryStore(control_path)

        store.register_symbol("ARXUSDT", BASELINE, now=NOW)
        registered_document = _read_document(control_path)

        assert registered_document["manual_unmanaged_switch"] == "keep-me"
        assert "recovery_action_lease_active" not in registered_document
        assert "recovery_action_lease_epoch" not in registered_document
        store.read("ARXUSDT")

        registered_document["recovery_action_lease_active"] = True
        control_path.write_text(
            json.dumps(registered_document),
            encoding="utf-8",
        )
        with pytest.raises(
            RecoveryStateCorruptError,
            match="unexpected managed recovery field",
        ):
            store.read("ARXUSDT")


def test_registration_preserves_unowned_legacy_recovery_fields() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        legacy_fields = {
            "recovery_active": True,
            "recovery_mode": "legacy-observe-only",
            "recovery_control_owner": "legacy-guard",
        }
        control_path.write_text(json.dumps(legacy_fields), encoding="utf-8")

        store = JsonRecoveryStore(control_path)
        registered = store.register_symbol("ARXUSDT", BASELINE, now=NOW)

        document = _read_document(control_path)
        assert {key: document[key] for key in legacy_fields} == legacy_fields
        assert store.read("ARXUSDT") == registered


def test_compare_and_swap_preserves_unmanaged_fields_and_increments_once() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        control_path.write_text(
            json.dumps({"manual_unmanaged_switch": {"mode": "manual"}}),
            encoding="utf-8",
        )
        store = JsonRecoveryStore(control_path)
        current = store.register_symbol("ARXUSDT", BASELINE, now=NOW)
        next_profile = materialize_profile(
            baseline=BASELINE,
            action_id=ActionId.MAKER_FLOW_RECOVER,
            action_updates={"recovery_order_side": "BUY"},
        )
        next_state = replace(
            current,
            document_revision=1,
            generation=1,
            desired_profile=next_profile,
            phase=RecoveryPhase.ACTIVE,
            active_action=ActionId.MAKER_FLOW_RECOVER,
            decision_id="maker-1",
            side=Side.BUY,
            order_role=OrderRole.ENTRY,
            issued_at=NOW,
            progress_deadline_at=NOW + timedelta(seconds=30),
            hard_expires_at=NOW + timedelta(minutes=1),
            last_round_id="round-1",
        )

        store.compare_and_swap(
            "ARXUSDT",
            expected_revision=0,
            next_state=next_state,
        )

        document = _read_document(control_path)
        assert document["manual_unmanaged_switch"] == {"mode": "manual"}
        assert document["recovery_order_side"] == "BUY"
        assert store.read("ARXUSDT") == next_state

        with pytest.raises(RuntimeError, match="revision conflict"):
            store.compare_and_swap(
                "ARXUSDT",
                expected_revision=0,
                next_state=next_state,
            )
        with pytest.raises(ValueError, match="exactly once"):
            store.compare_and_swap(
                "ARXUSDT",
                expected_revision=1,
                next_state=replace(next_state, document_revision=3),
            )


def test_safety_exit_state_round_trips_through_json_store() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        store = JsonRecoveryStore(control_path)
        state = store.register_symbol("ARXUSDT", BASELINE, now=NOW)
        engine = FuturesRecoveryDecisionEngine()
        entered = engine.plan_round(
            snapshot=SymbolSnapshot(
                symbol="ARXUSDT",
                captured_at=NOW,
                assessment=FlowBlockerAssessment(
                    safety_reasons=("volatility",),
                    safety_evidence_fingerprint="store-safety-1",
                    safety_observation_seq=1,
                    safety_observed_at=NOW,
                ),
            ),
            state=state,
            now=NOW,
            round_id="store-safety-enter",
        )
        store.compare_and_swap(
            "ARXUSDT",
            expected_revision=0,
            next_state=entered.next_state,
        )
        exited = engine.plan_round(
            snapshot=SymbolSnapshot(
                symbol="ARXUSDT",
                captured_at=NOW + timedelta(seconds=1),
                assessment=FlowBlockerAssessment(),
            ),
            state=store.read("ARXUSDT"),
            now=NOW + timedelta(seconds=1),
            round_id="store-safety-clear",
        )

        store.compare_and_swap(
            "ARXUSDT",
            expected_revision=1,
            next_state=exited.next_state,
        )
        reloaded = JsonRecoveryStore(control_path).read("ARXUSDT")

        assert reloaded.phase is RecoveryPhase.RESTORING
        assert reloaded.safety_lease is not None
        assert reloaded.safety_lease.evidence_fingerprint == "store-safety-1"
        assert "recovery_safety_lease_epoch" not in reloaded.desired_profile.fields


def test_new_store_instance_keeps_absolute_deadlines_after_restart() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        first_process = JsonRecoveryStore(control_path)
        initial = first_process.register_symbol("ARXUSDT", BASELINE, now=NOW)
        absolute_expiry = NOW + timedelta(seconds=47)
        safety_lease = SafetyLease(
            epoch=1,
            started_at=NOW,
            hard_expires_at=absolute_expiry,
            evidence_fingerprint="risk-1",
            observation_seq=1,
            observed_at=NOW,
        )
        safety_profile = materialize_profile(
            baseline=BASELINE,
            action_id=ActionId.SAFETY_CONVERGE,
            action_updates={
                "recovery_entry_blocked_sides": ["BUY", "SELL"],
                "recovery_reduce_only_allowed_sides": ["BUY", "SELL"],
            },
            safety_lease=safety_lease,
        )
        persisted = replace(
            initial,
            document_revision=1,
            generation=1,
            desired_profile=safety_profile,
            phase=RecoveryPhase.ACTIVE,
            active_action=ActionId.SAFETY_CONVERGE,
            decision_id="safety-1",
            issued_at=NOW,
            progress_deadline_at=NOW + timedelta(seconds=12),
            hard_expires_at=absolute_expiry,
            safety_lease=safety_lease,
            safety_lease_epoch=1,
        )
        first_process.compare_and_swap(
            "ARXUSDT",
            expected_revision=0,
            next_state=persisted,
        )

        after_restart = JsonRecoveryStore(control_path).read("ARXUSDT")

        assert after_restart.issued_at == NOW
        assert after_restart.progress_deadline_at == NOW + timedelta(seconds=12)
        assert after_restart.hard_expires_at == absolute_expiry
        assert after_restart.safety_lease is not None
        assert after_restart.safety_lease.hard_expires_at == absolute_expiry


def test_missing_or_corrupt_state_fails_closed_and_is_not_inferred_from_flat_control() -> (
    None
):
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        control_path.write_text(
            json.dumps({"best_quote_maker_volume_allow_loss_reduce_only": True}),
            encoding="utf-8",
        )
        store = JsonRecoveryStore(control_path)

        with pytest.raises(RecoveryStateUnavailableError):
            store.read("ARXUSDT")

        control_path.write_text("{not-json", encoding="utf-8")
        with pytest.raises(RecoveryStateCorruptError):
            store.read("ARXUSDT")
        with pytest.raises(RecoveryStateCorruptError):
            store.register_symbol("ARXUSDT", BASELINE, now=NOW)


def test_corrupt_embedded_state_cannot_be_overwritten_by_registration() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        control_path.write_text(
            json.dumps(
                {
                    "best_quote_maker_volume_allow_loss_reduce_only": True,
                    RECOVERY_STATE_KEY: {
                        "schema_version": 1,
                        "state": {"symbol": "ARXUSDT"},
                    },
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(RecoveryStateCorruptError):
            JsonRecoveryStore(control_path).register_symbol(
                "ARXUSDT", BASELINE, now=NOW
            )

        document = _read_document(control_path)
        assert document["best_quote_maker_volume_allow_loss_reduce_only"] is True


def test_embedded_state_missing_even_an_optional_field_fails_closed() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        store = JsonRecoveryStore(control_path)
        store.register_symbol("ARXUSDT", BASELINE, now=NOW)
        document = _read_document(control_path)
        envelope = document[RECOVERY_STATE_KEY]
        assert isinstance(envelope, dict)
        state_payload = envelope["state"]
        assert isinstance(state_payload, dict)
        del state_payload["hard_expires_at"]
        control_path.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(
            RecoveryStateCorruptError, match="missing fields.*hard_expires_at"
        ):
            JsonRecoveryStore(control_path).read("ARXUSDT")


def test_negative_persisted_lease_budget_fails_closed() -> None:
    payload = JsonRecoveryStore.encode_state(_full_state())
    payload["temporary_loss_lease_uses"]["episode-a"] = -1

    with pytest.raises(
        RecoveryStateCorruptError, match="temporary_loss_lease_uses.*non-negative"
    ):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    "state",
    (_active_temporary_loss_state(), _settling_temporary_loss_state()),
    ids=("active", "settling"),
)
@pytest.mark.parametrize("missing_field", ("progress_deadline_at", "hard_expires_at"))
def test_non_safety_recovery_phase_requires_both_absolute_deadlines(
    state: RecoveryState,
    missing_field: str,
) -> None:
    payload = JsonRecoveryStore.encode_state(state)
    payload[missing_field] = None

    with pytest.raises(RecoveryStateCorruptError, match=missing_field):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    "state",
    (_active_temporary_loss_state(), _settling_temporary_loss_state()),
    ids=("active", "settling"),
)
def test_temporary_loss_top_level_hard_expiry_must_match_typed_lease(
    state: RecoveryState,
) -> None:
    payload = JsonRecoveryStore.encode_state(state)
    payload["hard_expires_at"] = (NOW + timedelta(seconds=41)).isoformat()

    with pytest.raises(
        RecoveryStateCorruptError,
        match="hard_expires_at.*action_lease",
    ):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    "state",
    (_active_temporary_loss_state(), _settling_temporary_loss_state()),
    ids=("active", "settling"),
)
def test_temporary_loss_issue_time_must_match_typed_lease_start(
    state: RecoveryState,
) -> None:
    payload = JsonRecoveryStore.encode_state(state)
    payload["issued_at"] = (NOW - timedelta(seconds=19)).isoformat()

    with pytest.raises(
        RecoveryStateCorruptError,
        match="issued_at.*action_lease",
    ):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize("lease_field", ("side", "hard_expires_at"))
def test_active_temporary_loss_lease_must_match_profile_metadata(
    lease_field: str,
) -> None:
    payload = JsonRecoveryStore.encode_state(_active_temporary_loss_state())
    lease = payload["action_lease"]
    assert isinstance(lease, dict)
    if lease_field == "side":
        lease[lease_field] = Side.BUY.value
    else:
        lease[lease_field] = (NOW + timedelta(seconds=41)).isoformat()

    expected_error = (
        "active temporary loss lease"
        if lease_field == "side"
        else "hard_expires_at.*action_lease"
    )
    with pytest.raises(RecoveryStateCorruptError, match=expected_error):
        JsonRecoveryStore.decode_state(payload)


def test_settling_temporary_loss_order_target_must_match_typed_lease() -> None:
    state = _settling_temporary_loss_state()
    profile = materialize_profile(
        baseline=BASELINE,
        action_id=ActionId.TEMPORARY_LOSS_RELIEF,
        action_updates={
            "recovery_order_side": Side.BUY.value,
            "recovery_order_role": OrderRole.ENTRY.value,
        },
        action_lease=state.action_lease,
        action_lease_active=False,
    )

    with pytest.raises(
        RecoveryStateCorruptError,
        match="settling temporary loss lease metadata",
    ):
        JsonRecoveryStore.encode_state(replace(state, desired_profile=profile))


def test_active_temporary_loss_cannot_load_an_inactive_allow_loss_profile() -> None:
    payload = JsonRecoveryStore.encode_state(_settling_temporary_loss_state())
    payload["phase"] = RecoveryPhase.ACTIVE.value
    lease = payload["action_lease"]
    assert isinstance(lease, dict)
    lease["activated_at"] = (NOW - timedelta(seconds=19)).isoformat()

    with pytest.raises(RecoveryStateCorruptError, match="active temporary loss lease"):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    "state",
    (_active_safety_state(), _full_state()),
    ids=("active", "cleaning-successor"),
)
def test_profile_bound_safety_top_level_hard_expiry_must_match_typed_lease(
    state: RecoveryState,
) -> None:
    payload = JsonRecoveryStore.encode_state(state)
    payload["hard_expires_at"] = (NOW + timedelta(seconds=31)).isoformat()

    with pytest.raises(
        RecoveryStateCorruptError,
        match="hard_expires_at.*safety_lease",
    ):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    ("lease_field", "drift_value"),
    (
        ("evidence_fingerprint", "risk-seq-42"),
        ("observation_seq", 42),
        ("observed_at", NOW.isoformat()),
    ),
)
def test_profile_bound_safety_metadata_must_match_typed_lease(
    lease_field: str,
    drift_value: object,
) -> None:
    payload = JsonRecoveryStore.encode_state(_active_safety_state())
    lease = payload["safety_lease"]
    assert isinstance(lease, dict)
    lease[lease_field] = drift_value

    with pytest.raises(
        RecoveryStateCorruptError,
        match="safety lease does not match desired profile metadata",
    ):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    ("phase", "message"),
    (
        (RecoveryPhase.COOLDOWN, "cooldown_until"),
        (RecoveryPhase.RESTORING, "progress_deadline_at"),
        (RecoveryPhase.STOPPED, "terminal action"),
    ),
)
def test_phase_specific_required_fields_fail_closed(
    phase: RecoveryPhase,
    message: str,
) -> None:
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    payload = JsonRecoveryStore.encode_state(state)
    payload["phase"] = phase.value

    with pytest.raises(RecoveryStateCorruptError, match=message):
        JsonRecoveryStore.decode_state(payload)


@pytest.mark.parametrize(
    "state",
    (
        RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        _settling_temporary_loss_state(),
        _active_temporary_loss_state(),
        _active_safety_state(),
        _full_state(),
        _restoring_state(),
        _cooldown_state(),
        _stop_pending_state(),
        _stopped_state(),
    ),
    ids=(
        "stable",
        "settling",
        "active-loss",
        "active-safety",
        "cleaning",
        "restoring",
        "cooldown",
        "stop-pending",
        "stopped",
    ),
)
def test_each_legal_phase_combination_round_trips(state: RecoveryState) -> None:
    assert (
        JsonRecoveryStore.decode_state(JsonRecoveryStore.encode_state(state)) == state
    )


def _forbidden_phase_payload(case: str) -> dict[str, object]:
    active_loss = JsonRecoveryStore.encode_state(_active_temporary_loss_state())
    cleaning = JsonRecoveryStore.encode_state(_full_state())
    if case == "stable_with_action_lease":
        payload = JsonRecoveryStore.encode_state(
            RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
        )
        payload["action_lease"] = active_loss["action_lease"]
        payload["action_lease_epoch"] = 7
    elif case == "settling_with_cleanup":
        payload = JsonRecoveryStore.encode_state(_settling_temporary_loss_state())
        payload["cleanup_obligation"] = cleaning["cleanup_obligation"]
    elif case == "active_with_cleanup":
        payload = active_loss
        payload["cleanup_obligation"] = cleaning["cleanup_obligation"]
    elif case == "cleaning_without_cleanup":
        payload = cleaning
        payload["cleanup_obligation"] = None
    elif case == "restoring_with_action_lease":
        payload = JsonRecoveryStore.encode_state(_restoring_state())
        payload["action_lease"] = active_loss["action_lease"]
        payload["action_lease_epoch"] = 7
    elif case == "cooldown_with_cleanup":
        payload = JsonRecoveryStore.encode_state(_cooldown_state())
        payload["action_lease"] = active_loss["action_lease"]
        payload["action_lease_epoch"] = 7
        payload["cleanup_obligation"] = cleaning["cleanup_obligation"]
    elif case == "stop_pending_runner_running":
        payload = JsonRecoveryStore.encode_state(_stop_pending_state())
        payload["desired_runner_state"] = "running"
    elif case == "stopped_with_action_lease":
        payload = JsonRecoveryStore.encode_state(_stopped_state())
        payload["action_lease"] = active_loss["action_lease"]
        payload["action_lease_epoch"] = 7
    elif case == "active_with_terminal_confirmation":
        payload = active_loss
        payload["terminal_stop_confirmed"] = True
    elif case == "active_safety_without_safety_lease":
        payload = JsonRecoveryStore.encode_state(_active_safety_state())
        payload["safety_lease"] = None
    else:
        raise AssertionError(f"unknown forbidden phase case: {case}")
    return payload


@pytest.mark.parametrize(
    "case",
    (
        "stable_with_action_lease",
        "settling_with_cleanup",
        "active_with_cleanup",
        "cleaning_without_cleanup",
        "restoring_with_action_lease",
        "cooldown_with_cleanup",
        "stop_pending_runner_running",
        "stopped_with_action_lease",
        "active_with_terminal_confirmation",
        "active_safety_without_safety_lease",
    ),
)
def test_forbidden_phase_combinations_fail_closed(case: str) -> None:
    with pytest.raises(RecoveryStateCorruptError):
        JsonRecoveryStore.decode_state(_forbidden_phase_payload(case))


def test_pending_effect_stage_and_epoch_must_remain_paired() -> None:
    pending_without_epoch = JsonRecoveryStore.encode_state(_full_state())
    pending_without_epoch["pending_effect_epoch"] = None
    with pytest.raises(RecoveryStateCorruptError, match="pending effect"):
        JsonRecoveryStore.decode_state(pending_without_epoch)

    epoch_without_pending = JsonRecoveryStore.encode_state(
        RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    )
    epoch_without_pending["pending_effect_epoch"] = 1
    with pytest.raises(RecoveryStateCorruptError, match="pending_effect_epoch"):
        JsonRecoveryStore.decode_state(epoch_without_pending)


@pytest.mark.parametrize(
    ("drift_field", "drift_value"),
    (
        ("best_quote_maker_volume_allow_loss_reduce_only", False),
        ("recovery_action_lease_epoch", 999),
        ("volatility_entry_pause_enabled", None),
    ),
)
def test_flat_managed_control_drift_from_embedded_profile_fails_closed(
    drift_field: str,
    drift_value: object,
) -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        store = JsonRecoveryStore(control_path)
        registered = store.register_symbol("ARXUSDT", BASELINE, now=NOW)
        active = replace(
            _active_temporary_loss_state(),
            document_revision=1,
            baseline_profile=registered.baseline_profile,
        )
        store.compare_and_swap(
            "ARXUSDT",
            expected_revision=0,
            next_state=active,
        )
        document = _read_document(control_path)
        if drift_value is None:
            del document[drift_field]
        else:
            document[drift_field] = drift_value
        control_path.write_text(json.dumps(document), encoding="utf-8")

        with pytest.raises(
            RecoveryStateCorruptError, match="flat managed control drift"
        ):
            JsonRecoveryStore(control_path).read("ARXUSDT")
