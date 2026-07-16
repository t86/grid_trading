from __future__ import annotations

from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import patch

import pytest

from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    CleanupProof,
    EffectReceipt,
    EffectStage,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    RecoveryPhase,
    RecoveryPolicy,
    RecoveryState,
    Side,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_runtime_adapter import (
    RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY,
    RecoveryConfigAppliedReceiptJournalError,
    load_recovery_config_applied_evidence,
)
from grid_optimizer.loop_runner import (
    INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY,
    TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY,
    _load_recovery_order_profile_gate,
    _record_current_recovery_config_applied,
)
from grid_optimizer.semi_auto_plan import load_or_initialize_state


NOW = datetime(2026, 7, 16, 4, 0, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}


def _active_inventory_state(
    baseline: dict[str, object] | None = None,
) -> RecoveryState:
    state = RecoveryState.initial(
        "BCHUSDT",
        baseline or BASELINE,
        now=NOW,
    )
    return FuturesRecoveryDecisionEngine(
        policy=RecoveryPolicy(
            action_progress_timeout=timedelta(seconds=30),
            action_hard_ttl=timedelta(seconds=60),
        )
    ).plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=NOW,
            assessment=FlowBlockerAssessment(
                inventory_reduce_sides=(Side.SELL,),
            ),
        ),
        state=state,
        now=NOW,
        round_id="enter-inventory",
    ).next_state


def _args(state: RecoveryState, tmp_path, *, apply: bool = True) -> Namespace:
    return Namespace(
        symbol=state.symbol,
        apply=apply,
        recovery_control_path=str(tmp_path / "control.json"),
        recovery_generation=state.generation,
        state_path=str(tmp_path / "runner-state.json"),
        best_quote_maker_volume_allow_loss_reduce_only=False,
        best_quote_maker_volume_net_loss_reduce_enabled=False,
        hard_loss_forced_reduce_enabled=False,
        volatility_entry_pause_enabled=True,
    )


def test_runner_start_writes_strict_current_generation_config_receipt(
    tmp_path,
) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        first = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-bch-1",
        )
        second = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=2),
            runner_instance_id="runner-bch-1",
        )

    assert first is not None
    assert second is not None
    assert second["receipt_id"] == first["receipt_id"]
    document = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(document[RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY]["events"]) == 1
    evidence = load_recovery_config_applied_evidence(
        state_path=state_path,
        recovery_state=state,
        captured_at=NOW + timedelta(seconds=3),
    )
    assert evidence.applied_generation == state.generation
    assert evidence.applied_profile_digest == state.desired_profile.digest
    assert evidence.runner_instance_id == "runner-bch-1"


def test_corrupt_config_journal_self_heals_after_exact_profile_validation(
    tmp_path,
) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text(
        json.dumps(
            {
                "unrelated_runtime_state": {"must": "survive"},
                RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY: {"corrupt": True},
            }
        ),
        encoding="utf-8",
    )

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        receipt = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-self-heal",
        )

    assert receipt is not None
    assert receipt["journal_rebuilt"] is True
    document = json.loads(state_path.read_text(encoding="utf-8"))
    assert document["unrelated_runtime_state"] == {"must": "survive"}
    journal = document[RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY]
    assert journal["schema"] == "futures_recovery_config_applied_receipts_v1"
    assert len(journal["events"]) == 1
    evidence = load_recovery_config_applied_evidence(
        state_path=state_path,
        recovery_state=state,
        captured_at=NOW + timedelta(seconds=2),
    )
    assert evidence.applied_generation == state.generation
    assert evidence.applied_profile_digest == state.desired_profile.digest


def test_restoring_corrupt_config_journal_rebuilds_proof_and_reaches_cooldown(
    tmp_path,
) -> None:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=30),
        action_hard_ttl=timedelta(seconds=60),
        cleanup_retry_interval=timedelta(seconds=1),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(inventory_reduce_sides=(Side.SELL,))
    active = _active_inventory_state()
    effect_ack_at = NOW + timedelta(seconds=1)
    effect_ack = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=effect_ack_at,
            assessment=assessment,
            effect_receipt=EffectReceipt(
                decision_id=str(active.decision_id),
                stage=EffectStage.RUNNER_RESTART,
                effect_epoch=int(active.pending_effect_epoch),
                observed_at=effect_ack_at,
            ),
        ),
        state=active,
        now=effect_ack_at,
        round_id="config-corrupt-effect-ack",
    )
    deadline = effect_ack.next_state.progress_deadline_at
    assert deadline is not None
    cleaning = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=deadline,
            assessment=assessment,
        ),
        state=effect_ack.next_state,
        now=deadline,
        round_id="config-corrupt-timeout",
    )
    obligation = cleaning.next_state.cleanup_obligation
    assert cleaning.next_state.phase is RecoveryPhase.CLEANING
    assert obligation is not None
    cleaned_at = deadline + timedelta(seconds=1)
    restoring = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=cleaned_at,
            assessment=assessment,
            cleanup_proof=CleanupProof(
                source_decision_id=obligation.source_decision_id,
                source_generation=obligation.managed_order_manifest.generation,
                manifest_digest=obligation.managed_order_manifest.digest,
                remaining_order_ids=(),
                user_trades_watermark=0,
                observed_at=cleaned_at,
            ),
        ),
        state=cleaning.next_state,
        now=cleaned_at,
        round_id="config-corrupt-cleaned",
    )
    state = restoring.next_state
    assert state.phase is RecoveryPhase.RESTORING
    assert restoring.effect_stage is EffectStage.RUNNER_RESTART

    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text(
        json.dumps(
            {
                "unrelated_runtime_state": ["keep"],
                RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY: {
                    "schema": "tampered",
                    "events": "not-a-list",
                },
            }
        ),
        encoding="utf-8",
    )
    proof_at = cleaned_at + timedelta(seconds=1)
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        receipt = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=proof_at,
            runner_instance_id="runner-restoring-self-heal",
        )
    assert receipt is not None
    evidence = load_recovery_config_applied_evidence(
        state_path=state_path,
        recovery_state=state,
        captured_at=proof_at,
    )
    confirmed = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=proof_at,
            assessment=assessment,
            applied_generation=evidence.applied_generation,
            applied_profile_digest=evidence.applied_profile_digest,
        ),
        state=state,
        now=proof_at,
        round_id="config-corrupt-restored",
    )

    assert confirmed.next_state.phase is RecoveryPhase.COOLDOWN
    assert confirmed.effect_stage is EffectStage.NONE
    assert json.loads(state_path.read_text(encoding="utf-8"))[
        "unrelated_runtime_state"
    ] == ["keep"]


def test_runner_start_generation_mismatch_fails_without_receipt(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    args.recovery_generation -= 1
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        with pytest.raises(RuntimeError, match="generation"):
            _record_current_recovery_config_applied(
                args,
                state_path=state_path,
                now=NOW + timedelta(seconds=1),
                runner_instance_id="runner-stale",
            )

    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_runner_start_profile_mismatch_fails_without_receipt(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    args.volatility_entry_pause_enabled = False
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        with pytest.raises(RuntimeError, match="profile"):
            _record_current_recovery_config_applied(
                args,
                state_path=state_path,
                now=NOW + timedelta(seconds=1),
                runner_instance_id="runner-profile-drift",
            )

    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_old_generation_config_receipt_cannot_confirm_new_state(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-old",
        )
    new_state = replace(
        state,
        generation=state.generation + 1,
        decision_id="BCHUSDT:new-generation",
        issued_at=NOW + timedelta(seconds=5),
        progress_deadline_at=NOW + timedelta(seconds=35),
        hard_expires_at=NOW + timedelta(seconds=65),
    )

    evidence = load_recovery_config_applied_evidence(
        state_path=state_path,
        recovery_state=new_state,
        captured_at=NOW + timedelta(seconds=6),
    )

    assert evidence.applied_generation is None
    assert evidence.applied_profile_digest is None


def test_same_decision_multi_generation_history_selects_current_receipt(
    tmp_path,
) -> None:
    first_state = _active_inventory_state()
    args = _args(first_state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=first_state,
    ):
        _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-generation-1",
        )
    current_state = replace(
        first_state,
        generation=first_state.generation + 1,
    )
    args.recovery_generation = current_state.generation
    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=current_state,
    ):
        current = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=2),
            runner_instance_id="runner-generation-2",
        )

    evidence = load_recovery_config_applied_evidence(
        state_path=state_path,
        recovery_state=current_state,
        captured_at=NOW + timedelta(seconds=3),
    )

    assert current is not None
    assert evidence.applied_generation == current_state.generation
    assert evidence.receipt_id == current["receipt_id"]
    assert evidence.runner_instance_id == "runner-generation-2"


def test_same_generation_different_decision_config_receipt_fails_closed(
    tmp_path,
) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-conflicting-decision",
        )
    conflicting_state = replace(
        state,
        decision_id="BCHUSDT:other-decision-same-generation",
    )

    with pytest.raises(RecoveryConfigAppliedReceiptJournalError):
        load_recovery_config_applied_evidence(
            state_path=state_path,
            recovery_state=conflicting_state,
            captured_at=NOW + timedelta(seconds=2),
        )


def test_tampered_config_receipt_fails_closed(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-tampered",
        )
    document = json.loads(state_path.read_text(encoding="utf-8"))
    document[RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY]["events"][0][
        "profile_digest"
    ] = "profile-tampered"
    state_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(RecoveryConfigAppliedReceiptJournalError):
        load_recovery_config_applied_evidence(
            state_path=state_path,
            recovery_state=state,
            captured_at=NOW + timedelta(seconds=2),
        )


def test_config_receipt_at_progress_deadline_is_rejected(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        with pytest.raises(RuntimeError, match="deadline"):
            _record_current_recovery_config_applied(
                args,
                state_path=state_path,
                now=state.progress_deadline_at,
                runner_instance_id="runner-late",
            )

    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_no_apply_runner_does_not_emit_config_applied_proof(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path, apply=False)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        receipt = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-dry",
        )

    assert receipt is None
    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_real_unregistered_control_path_remains_unmanaged(tmp_path) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    args.recovery_generation = None
    control_path = tmp_path / "control.json"
    control_path.write_text(
        json.dumps(
            {
                "symbol": state.symbol,
                "apply": True,
                "volatility_entry_pause_enabled": True,
            }
        ),
        encoding="utf-8",
    )

    gate = _load_recovery_order_profile_gate(
        args,
        now=NOW + timedelta(seconds=1),
    )

    assert gate.managed is False
    assert gate.ready is True
    assert gate.reason is None


def test_real_unregistered_apply_runner_writes_no_recovery_receipt(
    tmp_path,
) -> None:
    state = _active_inventory_state()
    args = _args(state, tmp_path)
    args.recovery_generation = None
    control_path = tmp_path / "control.json"
    control_path.write_text(
        json.dumps({"symbol": state.symbol, "apply": True}),
        encoding="utf-8",
    )
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    receipt = _record_current_recovery_config_applied(
        args,
        state_path=state_path,
        now=NOW + timedelta(seconds=1),
        runner_instance_id="runner-unregistered",
    )

    assert receipt is None
    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_config_receipt_rejects_cycle_budget_drift(tmp_path) -> None:
    state = _active_inventory_state(
        {
            **BASELINE,
            "best_quote_maker_volume_cycle_budget_notional": 200.0,
        }
    )
    args = _args(state, tmp_path)
    args.best_quote_maker_volume_cycle_budget_notional = 150.0
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        with pytest.raises(RuntimeError, match="profile"):
            _record_current_recovery_config_applied(
                args,
                state_path=state_path,
                now=NOW + timedelta(seconds=1),
                runner_instance_id="runner-budget-drift",
            )

    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_config_receipt_rejects_unknown_non_overlay_profile_field(
    tmp_path,
) -> None:
    state = _active_inventory_state(
        {
            **BASELINE,
            "unregistered_runtime_profile_field": "must-not-be-guessed",
        }
    )
    args = _args(state, tmp_path)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        with pytest.raises(RuntimeError, match="profile"):
            _record_current_recovery_config_applied(
                args,
                state_path=state_path,
                now=NOW + timedelta(seconds=1),
                runner_instance_id="runner-unknown-profile",
            )

    assert RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_config_receipt_normalizes_list_and_tuple_runtime_values(tmp_path) -> None:
    state = _active_inventory_state(
        {
            **BASELINE,
            "managed_side_sequence": ["BUY", "SELL"],
        }
    )
    args = _args(state, tmp_path)
    args.managed_side_sequence = ("BUY", "SELL")
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch("grid_optimizer.loop_runner.JsonRecoveryStore.read", return_value=state):
        receipt = _record_current_recovery_config_applied(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=1),
            runner_instance_id="runner-sequence-normalized",
        )

    assert receipt is not None


def test_reset_state_preserves_only_recovery_receipt_journals(tmp_path) -> None:
    state_path = tmp_path / "runner-state.json"
    journals = {
        RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY: {"schema": "config-proof"},
        INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY: {"schema": "inventory-proof"},
        TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY: {"schema": "loss-proof"},
    }
    state_path.write_text(
        json.dumps(
            {
                **journals,
                "volatility_entry_pause_state": {"active": True},
                "runtime_guard_loss_recovery": {"active": True},
                "temporary_strategy_flag": True,
            }
        ),
        encoding="utf-8",
    )
    planner_args = Namespace(
        symbol="BCHUSDT",
        strategy_mode="hedge_best_quote_maker_volume_v1",
        step_price=0.1,
        buy_levels=1,
        sell_levels=1,
        per_order_notional=10.0,
        base_position_notional=20.0,
        center_price=None,
        down_trigger_steps=1,
        up_trigger_steps=1,
        shift_steps=1,
    )

    reset = load_or_initialize_state(
        state_path=state_path,
        args=planner_args,
        symbol_info={
            "tick_size": 0.01,
            "step_size": 0.1,
            "min_qty": 0.1,
            "min_notional": 5.0,
        },
        mid_price=100.0,
        reset_state=True,
    )

    for key, journal in journals.items():
        assert reset[key] == journal
    assert "volatility_entry_pause_state" not in reset
    assert "runtime_guard_loss_recovery" not in reset
    assert "temporary_strategy_flag" not in reset
