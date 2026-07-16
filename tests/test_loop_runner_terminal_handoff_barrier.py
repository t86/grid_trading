from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from grid_optimizer import bq_volume_recovery_guard
from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    ActivationReceipt,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    ManagedOrderIdentity,
    ManagedOrderManifest,
    OrderRole,
    RecoveryPhase,
    Side,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_store import JsonRecoveryStore
from grid_optimizer.futures_run_lifecycle import (
    bind_run_contract_owner,
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.futures_terminal_ownership import terminal_intent_id
from grid_optimizer.loop_runner import (
    _maybe_handle_runtime_guard,
    _terminal_intent_runtime_args,
)


NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)


def _registered_bounded_args(tmp_path: Path) -> tuple[argparse.Namespace, JsonRecoveryStore]:
    args = argparse.Namespace(
        symbol="BCHUSDT",
        strategy_profile="bch-volume-v1",
        strategy_mode="hedge_best_quote_maker_volume_v1",
        per_order_notional=20.0,
        terminal_drain_max_order_notional=20.0,
        terminal_drain_loss_lease_seconds=300.0,
        terminal_drain_order_reprice_seconds=120.0,
        terminal_drain_flat_confirm_cycles=2,
        run_start_time="2026-07-16T00:00:00+00:00",
        runtime_guard_stats_start_time="2026-07-16T00:00:00+00:00",
        run_end_time="2026-07-17T00:00:00+00:00",
        max_cumulative_notional=20_000.0,
        terminal_drain_exit_policy="drain_then_preserve",
        terminal_drain_absolute_loss_budget=2.0,
        terminal_drain_max_wait_seconds=600.0,
        terminal_drain_stop_preserve_reason=None,
        lifecycle_wear_stop_per_10k=None,
        lifecycle_wear_stop_min_gross_notional=None,
        rolling_hourly_loss_limit=None,
        rolling_hourly_loss_per_10k_limit=None,
        rolling_hourly_loss_per_10k_min_notional=None,
        max_actual_net_notional=None,
        max_synthetic_drift_notional=None,
        max_unrealized_loss=None,
        state_path=str(tmp_path / "bchusdt_loop_state.json"),
        plan_json=str(tmp_path / "bchusdt_loop_latest_plan.json"),
        recv_window=5000,
    )
    control, _owner = bind_run_contract_owner(
        vars(args),
        activated_at=NOW - timedelta(hours=1),
    )
    control_path = tmp_path / "bchusdt_loop_runner_control.json"
    control_path.write_text(json.dumps(control), encoding="utf-8")
    store = JsonRecoveryStore(control_path)
    registered = store.register_symbol(
        "BCHUSDT",
        {
            "best_quote_maker_volume_allow_loss_reduce_only": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "hard_loss_forced_reduce_enabled": False,
            "volatility_entry_pause_enabled": True,
        },
        now=NOW - timedelta(minutes=10),
    )
    args.recovery_control_path = str(control_path)
    args.recovery_generation = registered.generation
    Path(args.state_path).write_text("{}", encoding="utf-8")
    return args, store


def _write_terminal_intent(args: argparse.Namespace, *, requested_at: datetime) -> Path:
    snapshot = run_contract_snapshot_from_config(vars(args))
    target = float(snapshot["max_cumulative_notional"])
    contract_id = run_contract_identity_from_config(snapshot)
    path = Path(args.state_path).parent / "bchusdt_terminal_intent.json"
    path.write_text(
        json.dumps(
            {
                "schema": "futures_lifecycle_intent_v2",
                "intent_id": terminal_intent_id(
                    symbol="BCHUSDT",
                    source="competition_target_gate",
                    trigger_reason="target_reached",
                    run_contract_id=contract_id,
                ),
                "symbol": "BCHUSDT",
                "source": "competition_target_gate",
                "action": "lifecycle_drain",
                "trigger_reason": "target_reached",
                "requested_at": requested_at.isoformat(),
                "status": "pending",
                "exit_policy": "use_immutable_run_contract",
                "run_contract_id": contract_id,
                "run_contract_snapshot": snapshot,
                "observed": {
                    "gross_notional": target + 1.0,
                    "target": target,
                    "realized_pnl": 0.0,
                    "wear_per_10k": 0.0,
                    "trade_count": 1,
                    "window_start": snapshot["runtime_guard_stats_start_time"],
                    "window_end": snapshot["run_end_time"],
                    "query_end": requested_at.isoformat(),
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def _enter_terminal_cleanup(store: JsonRecoveryStore):
    engine = FuturesRecoveryDecisionEngine()
    initial = store.read("BCHUSDT").with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW - timedelta(seconds=4),
    )
    pressure = FlowBlockerAssessment(
        inventory_reduce_sides=(Side.SELL,),
        loss_only_blocked_sides=(Side.SELL,),
        episode_fingerprint="terminal-handoff-barrier",
    )
    settling = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="BCHUSDT",
            captured_at=NOW - timedelta(seconds=3),
            assessment=pressure,
        ),
        state=initial,
        now=NOW - timedelta(seconds=3),
        round_id="barrier-enter",
    ).next_state
    store.compare_and_swap(
        "BCHUSDT",
        expected_revision=initial.document_revision,
        next_state=settling,
    )
    assert settling.action_lease is not None
    activated_at = NOW - timedelta(seconds=2)
    active = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="BCHUSDT",
            captured_at=activated_at,
            assessment=pressure,
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
        round_id="barrier-activate",
    ).next_state
    store.compare_and_swap(
        "BCHUSDT",
        expected_revision=settling.document_revision,
        next_state=active,
    )
    cleanup_at = NOW - timedelta(seconds=1)
    assert active.decision_id is not None
    assert active.issued_at is not None
    assert active.side is not None
    assert active.order_role is not None
    assert active.action_lease is not None
    manifest = ManagedOrderManifest(
        symbol=active.symbol,
        generation=active.generation,
        decision_id=active.decision_id,
        profile_digest=active.desired_profile.digest,
        action_id=active.active_action,
        side=active.side,
        order_role=active.order_role,
        issued_at=active.issued_at,
        orders=(
            ManagedOrderIdentity(
                order_id="701",
                client_order_id="typed-loss-701",
            ),
        ),
        action_lease_epoch=active.action_lease.epoch,
        hard_expires_at=active.action_lease.hard_expires_at,
    )
    cleaning = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="BCHUSDT",
            captured_at=cleanup_at,
            assessment=FlowBlockerAssessment(
                terminal_reason="trusted_lifecycle_terminal_handoff",
                terminal_lifecycle_handoff=True,
            ),
            managed_order_manifest=manifest,
        ),
        state=active,
        now=cleanup_at,
        round_id="barrier-cleanup",
    ).next_state
    store.compare_and_swap(
        "BCHUSDT",
        expected_revision=active.document_revision,
        next_state=cleaning,
    )
    assert cleaning.phase is RecoveryPhase.CLEANING
    assert cleaning.action_lease is not None
    assert cleaning.cleanup_obligation is not None
    return cleaning


def test_registered_terminal_intent_waits_for_coordinator_cleanup(tmp_path: Path) -> None:
    args, store = _registered_bounded_args(tmp_path)
    cleaning = _enter_terminal_cleanup(store)
    args.recovery_generation = cleaning.generation
    intent_path = _write_terminal_intent(args, requested_at=NOW)
    state_path = Path(args.state_path)
    intent_before = intent_path.read_bytes()
    state_before = state_path.read_bytes()
    control_before = Path(args.recovery_control_path).read_bytes()

    with (
        patch(
            "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
            side_effect=AssertionError("terminal barrier read exchange state"),
        ) as exchange_inputs,
        patch(
            "grid_optimizer.loop_runner.evaluate_runtime_guards",
            side_effect=AssertionError("terminal barrier evaluated lifecycle"),
        ) as lifecycle,
        patch(
            "grid_optimizer.loop_runner._handle_terminal_drain_round",
            side_effect=AssertionError("terminal drain ran before recovery handoff"),
        ) as terminal_drain,
    ):
        summary = _maybe_handle_runtime_guard(
            args=args,
            cycle=1,
            cycle_started_at=NOW,
            summary_path=tmp_path / "events.jsonl",
        )

    assert summary["runtime_status"] == "handoff_pending"
    assert summary["terminal_drain_action"] == "hold_blocked"
    assert summary["stop_triggered"] is False
    assert summary["runner_exit_allowed"] is False
    assert summary["global_allow_loss"] is False
    assert summary["recovery_phase"] == "cleaning"
    assert intent_path.read_bytes() == intent_before
    assert state_path.read_bytes() == state_before
    assert Path(args.recovery_control_path).read_bytes() == control_before
    exchange_inputs.assert_not_called()
    lifecycle.assert_not_called()
    terminal_drain.assert_not_called()


@pytest.mark.parametrize(
    ("trigger", "cycle_started_at", "gross_notional", "primary_reason"),
    (
        ("target_reached", NOW, 20_001.0, "max_cumulative_notional_hit"),
        (
            "target_unmet_deadline",
            datetime(2026, 7, 17, tzinfo=timezone.utc),
            10_000.0,
            "after_end_window",
        ),
    ),
)
def test_internal_terminal_condition_persists_handoff_before_drain(
    tmp_path: Path,
    trigger: str,
    cycle_started_at: datetime,
    gross_notional: float,
    primary_reason: str,
) -> None:
    args, store = _registered_bounded_args(tmp_path)
    cleaning = _enter_terminal_cleanup(store)
    args.recovery_generation = cleaning.generation
    args.max_actual_net_notional = 100.0
    matched_reasons = [primary_reason]
    runtime_result = argparse.Namespace(
        tradable=False,
        runtime_status="stopped",
        stop_triggered=True,
        primary_reason=primary_reason,
        matched_reasons=matched_reasons,
        triggered_at=cycle_started_at.isoformat(),
        rolling_hourly_loss=0.0,
        rolling_hourly_gross_notional=gross_notional,
        rolling_hourly_loss_per_10k=0.0,
        rolling_hourly_loss_per_10k_active=False,
        cumulative_gross_notional=gross_notional,
        actual_net_notional_abs=101.0,
        synthetic_drift_notional=0.0,
        unrealized_loss=0.0,
    )

    with (
        patch(
            "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
            return_value=(
                gross_notional,
                [],
                datetime(2026, 7, 16, tzinfo=timezone.utc),
            ),
        ) as exchange_inputs,
        patch(
            "grid_optimizer.loop_runner.evaluate_runtime_guards",
            return_value=runtime_result,
        ),
        patch(
            "grid_optimizer.loop_runner._handle_terminal_drain_round",
            side_effect=AssertionError("internal condition drained before handoff"),
        ) as terminal_drain,
    ):
        summary = _maybe_handle_runtime_guard(
            args=args,
            cycle=1,
            cycle_started_at=cycle_started_at,
            summary_path=tmp_path / "events.jsonl",
        )

    intent_path = tmp_path / "bchusdt_terminal_intent.json"
    intent = json.loads(intent_path.read_text(encoding="utf-8"))
    _effective, validation_error = _terminal_intent_runtime_args(
        args=args,
        terminal_intent=intent,
    )
    delegated = bq_volume_recovery_guard._terminal_drain_delegation(
        symbol="BCHUSDT",
        output_dir=tmp_path,
        dry_run=False,
    )

    assert summary["runtime_status"] == "handoff_pending"
    assert summary["terminal_drain_action"] == "hold_blocked"
    assert summary["runner_exit_allowed"] is False
    assert summary["global_allow_loss"] is False
    assert intent["status"] == "pending"
    assert intent["trigger_reason"] == trigger
    assert validation_error is None
    assert delegated["action"] == "delegated_to_terminal_drain_intent"
    exchange_inputs.assert_called_once()
    terminal_drain.assert_not_called()

    with patch(
        "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
        side_effect=AssertionError("pending handoff re-read exchange"),
    ) as second_exchange:
        second = _maybe_handle_runtime_guard(
            args=args,
            cycle=2,
            cycle_started_at=cycle_started_at + timedelta(seconds=1),
            summary_path=tmp_path / "events.jsonl",
        )
    assert second["runtime_status"] == "handoff_pending"
    second_exchange.assert_not_called()


def test_registered_terminal_intent_runs_after_recovery_is_stable(tmp_path: Path) -> None:
    args, store = _registered_bounded_args(tmp_path)
    stable = store.read("BCHUSDT")
    args.recovery_generation = stable.generation
    intent_path = _write_terminal_intent(args, requested_at=NOW)
    with (
        patch(
            "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
            return_value=(20_001.0, [], NOW - timedelta(hours=1)),
        ),
        patch(
            "grid_optimizer.loop_runner.evaluate_runtime_guards",
            return_value=argparse.Namespace(
                tradable=True,
                runtime_status="running",
                stop_triggered=False,
                primary_reason=None,
                matched_reasons=[],
                triggered_at=None,
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=0.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=20_001.0,
                unrealized_loss=0.0,
            ),
        ),
        patch(
            "grid_optimizer.loop_runner._handle_terminal_drain_round",
            return_value={
                "runtime_status": "draining",
                "stop_triggered": False,
                "runner_exit_allowed": False,
                "terminal_drain_action": "block_entry",
            },
        ) as terminal_drain,
    ):
        summary = _maybe_handle_runtime_guard(
            args=args,
            cycle=2,
            cycle_started_at=NOW,
            summary_path=tmp_path / "events.jsonl",
        )

    assert summary["runtime_status"] == "draining"
    terminal_drain.assert_called_once()
    assert json.loads(intent_path.read_text(encoding="utf-8"))["status"] == "accepted"


def test_registered_risk_condition_signals_recovery_instead_of_terminal_handoff(
    tmp_path: Path,
) -> None:
    args, store = _registered_bounded_args(tmp_path)
    cleaning = _enter_terminal_cleanup(store)
    args.recovery_generation = cleaning.generation
    args.max_actual_net_notional = 100.0
    state_path = Path(args.state_path)
    with (
        patch(
            "grid_optimizer.loop_runner._load_futures_runtime_guard_inputs",
            return_value=(10_000.0, [], NOW - timedelta(hours=1)),
        ),
        patch(
            "grid_optimizer.loop_runner.evaluate_runtime_guards",
            return_value=argparse.Namespace(
                tradable=False,
                runtime_status="stopped",
                stop_triggered=True,
                primary_reason="max_actual_net_notional_hit",
                matched_reasons=["max_actual_net_notional_hit"],
                triggered_at=NOW.isoformat(),
                rolling_hourly_loss=0.0,
                rolling_hourly_gross_notional=10_000.0,
                rolling_hourly_loss_per_10k=0.0,
                rolling_hourly_loss_per_10k_active=False,
                cumulative_gross_notional=10_000.0,
                actual_net_notional_abs=101.0,
                synthetic_drift_notional=0.0,
                unrealized_loss=0.0,
            ),
        ),
        patch(
            "grid_optimizer.loop_runner._handle_terminal_drain_round",
            side_effect=AssertionError("recoverable risk condition drained"),
        ) as terminal_drain,
    ):
        summary = _maybe_handle_runtime_guard(
            args=args,
            cycle=1,
            cycle_started_at=NOW,
            summary_path=tmp_path / "events.jsonl",
        )

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    signal = persisted["futures_recovery_runtime_safety_signal"]
    assert summary is None
    assert signal["conditions"][0]["reason"] == "max_actual_net_notional_hit"
    assert not (tmp_path / "bchusdt_terminal_intent.json").exists()
    terminal_drain.assert_not_called()


def test_internal_terminal_intent_rejects_tampered_deadline_and_condition_proof(
    tmp_path: Path,
) -> None:
    args, _store = _registered_bounded_args(tmp_path)
    intent_path = _write_terminal_intent(args, requested_at=NOW)
    base = json.loads(intent_path.read_text(encoding="utf-8"))

    deadline = json.loads(json.dumps(base))
    deadline["trigger_reason"] = "target_unmet_deadline"
    deadline["requested_at"] = "2026-07-17T00:00:00+00:00"
    deadline["observed"].update(
        {
            "gross_notional": 10_000.0,
            "query_end": "2026-07-16T23:59:59+00:00",
            "runtime_guard_primary_reason": "after_end_window",
            "runtime_guard_matched_reasons": ["after_end_window"],
        }
    )
    _effective, deadline_error = _terminal_intent_runtime_args(
        args=args,
        terminal_intent=deadline,
    )

    condition = json.loads(json.dumps(base))
    condition["trigger_reason"] = "condition_unmet"
    condition["observed"].update(
        {
            "gross_notional": 10_000.0,
            "runtime_guard_primary_reason": "max_actual_net_notional_hit",
            "runtime_guard_matched_reasons": ["after_end_window"],
        }
    )
    _effective, condition_error = _terminal_intent_runtime_args(
        args=args,
        terminal_intent=condition,
    )

    assert deadline_error == "terminal_intent_deadline_proof_invalid"
    assert condition_error == "terminal_intent_trigger_reason_invalid"
