from __future__ import annotations

from argparse import Namespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    ActivationReceipt,
    CleanupProof,
    EffectReceipt,
    EffectStage,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    OrderRole,
    ProgressReceipt,
    RecoveryPolicy,
    RecoveryState,
    Side,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_store import (
    JsonRecoveryStore,
    RECOVERY_STATE_MIRROR_KEY,
    RECOVERY_STATE_KEY,
    RECOVERY_STATE_SCHEMA_VERSION,
)
from grid_optimizer.futures_recovery_runtime_adapter import (
    temporary_loss_client_order_prefix,
)
from grid_optimizer.loop_runner import (
    TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY,
    _best_quote_submit_allow_loss_roles,
    _best_quote_take_profit_guard_role_sets,
    _build_temporary_loss_client_order_id,
    _guard_temporary_loss_order_before_submit,
    _build_parser,
    _maintain_temporary_loss_recovery_receipts,
    _record_temporary_loss_recovery_progress,
    _temporary_loss_runtime_allow_roles,
    apply_take_profit_profit_guard,
    main,
)
from grid_optimizer.web import RUNNER_DEFAULT_CONFIG, _build_runner_command


NOW = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}


def _loss_states(*, side: Side = Side.SELL) -> tuple[RecoveryState, RecoveryState]:
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=30),
        action_hard_ttl=timedelta(seconds=60),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(side,),
        loss_only_blocked_sides=(side,),
        episode_fingerprint=f"lease-{side.value.lower()}",
    )
    state = RecoveryState.initial("ARXUSDT", BASELINE, now=NOW)
    state = state.with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        side,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )
    entered = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=NOW,
            assessment=assessment,
        ),
        state=state,
        now=NOW,
        round_id="enter-loss-lease",
    )
    settling = entered.next_state
    assert settling.action_lease is not None
    activated_at = NOW + timedelta(seconds=1)
    activated = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol="ARXUSDT",
            captured_at=activated_at,
            assessment=assessment,
            activation_receipt=ActivationReceipt(
                decision_id=settling.decision_id,
                generation=settling.generation,
                profile_digest=settling.desired_profile.digest,
                action_lease_epoch=settling.action_lease.epoch,
                observed_at=activated_at,
            ),
        ),
        state=settling,
        now=activated_at,
        round_id="activate-loss-lease",
    ).next_state
    assert activated.action_lease is not None
    return settling, activated


def _temporary_loss_client_order_id(
    state: RecoveryState,
    *,
    suffix: str = "a1b2c3",
) -> str:
    prefix = temporary_loss_client_order_prefix(state)
    return prefix + suffix[: 36 - len(prefix)]


def _active_loss_state(*, side: Side = Side.SELL) -> RecoveryState:
    return _loss_states(side=side)[1]


def _restoring_loss_state(*, side: Side = Side.SELL) -> RecoveryState:
    active = _active_loss_state(side=side)
    policy = RecoveryPolicy(
        action_progress_timeout=timedelta(seconds=30),
        action_hard_ttl=timedelta(seconds=60),
    )
    engine = FuturesRecoveryDecisionEngine(policy=policy)
    assessment = FlowBlockerAssessment(
        inventory_reduce_sides=(side,),
        loss_only_blocked_sides=(side,),
        episode_fingerprint=f"lease-{side.value.lower()}",
    )
    cleaning = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=active.symbol,
            captured_at=NOW + timedelta(seconds=2),
            assessment=assessment,
                progress_receipts=(
                ProgressReceipt(
                    decision_id=str(active.decision_id),
                    generation=active.generation,
                    side=side,
                    order_role=OrderRole.REDUCE_ONLY,
                    observation_seq=1,
                    observed_at=NOW + timedelta(seconds=2),
                    ),
                ),
                effect_receipt=EffectReceipt(
                    decision_id=str(active.decision_id),
                    stage=EffectStage.RUNNER_RESTART,
                    effect_epoch=int(active.pending_effect_epoch),
                    observed_at=NOW + timedelta(seconds=2),
                ),
            ),
        state=active,
        now=NOW + timedelta(seconds=2),
        round_id="exit-loss-lease",
    ).next_state
    assert cleaning.cleanup_obligation is not None
    obligation = cleaning.cleanup_obligation
    restoring = engine.plan_round(
        snapshot=SymbolSnapshot(
            symbol=cleaning.symbol,
            captured_at=NOW + timedelta(seconds=3),
            assessment=assessment,
            cleanup_proof=CleanupProof(
                source_decision_id=obligation.source_decision_id,
                source_generation=(
                    obligation.managed_order_manifest.generation
                ),
                manifest_digest=obligation.managed_order_manifest.digest,
                remaining_order_ids=(),
                user_trades_watermark=0,
                observed_at=NOW + timedelta(seconds=3),
            ),
        ),
        state=cleaning,
        now=NOW + timedelta(seconds=3),
        round_id="clean-loss-lease",
    ).next_state
    assert restoring.action_lease is None
    return restoring


def _args(state: RecoveryState) -> Namespace:
    return Namespace(
        symbol=state.symbol,
        best_quote_maker_volume_allow_loss_reduce_only=True,
        best_quote_maker_volume_net_loss_reduce_enabled=False,
        hard_loss_forced_reduce_enabled=False,
        volatility_entry_pause_enabled=True,
        best_quote_maker_volume_active_pair_reduce_enabled=False,
        recovery_control_path="/unused/in/unit/test.json",
        recovery_generation=state.generation,
        state_path="/unused/in/unit/runner-state.json",
    )


def _authorization_binding(state: RecoveryState) -> dict[str, object]:
    assert state.action_lease is not None
    role = (
        "best_quote_reduce_long"
        if state.action_lease.side is Side.SELL
        else "best_quote_reduce_short"
    )
    return {
        "symbol": state.symbol,
        "generation": state.generation,
        "profile_digest": state.desired_profile.digest,
        "decision_id": state.decision_id,
        "action_lease_epoch": state.action_lease.epoch,
        "side": state.action_lease.side.value,
        "role": role,
        "hard_expires_at": state.action_lease.hard_expires_at.isoformat(),
    }


@pytest.mark.parametrize(
    "state_mutator, now, expected_generation_delta",
    [
        (lambda state: replace(state, action_lease=None), NOW + timedelta(seconds=2), 0),
        (lambda state: state, NOW + timedelta(seconds=60), 0),
        (lambda state: replace(state, side=Side.BUY), NOW + timedelta(seconds=2), 0),
        (lambda state: replace(state, order_role=OrderRole.ENTRY), NOW + timedelta(seconds=2), 0),
        (lambda state: replace(state, symbol="OUSDT"), NOW + timedelta(seconds=2), 0),
        (
            lambda state: replace(state, generation=state.generation + 1),
            NOW + timedelta(seconds=2),
            0,
        ),
    ],
    ids=(
        "missing",
        "expired",
        "side_mismatch",
        "role_mismatch",
        "symbol_mismatch",
        "generation_mismatch",
    ),
)
def test_raw_allow_loss_cannot_bypass_invalid_typed_lease(
    state_mutator,
    now: datetime,
    expected_generation_delta: int,
) -> None:
    active = _active_loss_state(side=Side.SELL)
    observed = state_mutator(active)
    args = _args(active)
    expected_authorization = _authorization_binding(active)
    expected_authorization["generation"] = (
        active.generation + expected_generation_delta
    )

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=observed,
    ):
        roles = _temporary_loss_runtime_allow_roles(
            args,
            now=now,
            expected_authorization=expected_authorization,
            require_expected_authorization=True,
        )
        submit_roles = _best_quote_submit_allow_loss_roles(
            args,
            now=now,
            expected_authorization=expected_authorization,
            require_expected_authorization=True,
        )

    assert roles == frozenset()
    assert "best_quote_reduce_long" not in submit_roles
    assert "best_quote_reduce_short" not in submit_roles


def test_valid_typed_lease_opens_only_its_reduce_side() -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        roles = _temporary_loss_runtime_allow_roles(
            args,
            now=NOW + timedelta(seconds=2),
            expected_authorization=_authorization_binding(active),
            require_expected_authorization=True,
        )
        submit_roles = _best_quote_submit_allow_loss_roles(
            args,
            now=NOW + timedelta(seconds=2),
            expected_authorization=_authorization_binding(active),
            require_expected_authorization=True,
        )

    assert roles == frozenset({"best_quote_reduce_long"})
    assert "best_quote_reduce_long" in submit_roles
    assert "best_quote_reduce_short" not in submit_roles


def test_runner_reads_valid_lease_from_symbol_control(tmp_path) -> None:
    active = _active_loss_state(side=Side.BUY)
    control_path = tmp_path / "arxusdt_loop_runner_control.json"
    document = dict(active.desired_profile.fields)
    document[RECOVERY_STATE_KEY] = {
        "schema_version": RECOVERY_STATE_SCHEMA_VERSION,
        "state": JsonRecoveryStore.encode_state(active),
    }
    document[RECOVERY_STATE_MIRROR_KEY] = document[RECOVERY_STATE_KEY]
    control_path.write_text(json.dumps(document), encoding="utf-8")
    args = _args(active)
    args.recovery_control_path = str(control_path)

    roles = _temporary_loss_runtime_allow_roles(
        args,
        now=NOW + timedelta(seconds=2),
    )

    assert roles == frozenset({"best_quote_reduce_short"})


def test_active_generation_rejects_old_runner_and_accepts_restarted_runner() -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    args.recovery_generation = active.generation - 1

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        stale_roles = _temporary_loss_runtime_allow_roles(
            args,
            now=NOW + timedelta(seconds=2),
            expected_authorization=_authorization_binding(active),
            require_expected_authorization=True,
        )
        args.recovery_generation = active.generation
        restarted_roles = _temporary_loss_runtime_allow_roles(
            args,
            now=NOW + timedelta(seconds=2),
            expected_authorization=_authorization_binding(active),
            require_expected_authorization=True,
        )

    assert stale_roles == frozenset()
    assert restarted_roles == frozenset({"best_quote_reduce_long"})


def test_saved_runner_command_pins_control_path_and_recovery_generation() -> None:
    config = dict(RUNNER_DEFAULT_CONFIG)
    state = replace(
        RecoveryState.initial(
            "ARXUSDT",
            {
                **BASELINE,
                "best_quote_maker_volume_same_side_entry_price_guard_report_only": config[
                    "best_quote_maker_volume_same_side_entry_price_guard_report_only"
                ],
            },
            now=NOW,
        ),
        generation=7,
    )
    envelope = {
        "schema_version": RECOVERY_STATE_SCHEMA_VERSION,
        "state": JsonRecoveryStore.encode_state(state),
    }
    config.update(state.desired_profile.fields)
    config.update(
        {
            "symbol": "ARXUSDT",
            RECOVERY_STATE_KEY: envelope,
            RECOVERY_STATE_MIRROR_KEY: envelope,
        }
    )

    command = _build_runner_command(config)

    control_index = command.index("--recovery-control-path")
    generation_index = command.index("--recovery-generation")
    assert command[control_index + 1].endswith("arxusdt_loop_runner_control.json")
    assert command[generation_index + 1] == "7"


def _apply_loss_guard(authorized_roles: frozenset[str]) -> tuple[dict, dict]:
    long_roles, short_roles = _best_quote_take_profit_guard_role_sets(
        hedge_best_quote=True,
        enabled=True,
        allow_loss_reduce_only=True,
        authorized_loss_roles=authorized_roles,
    )
    plan = {
        "bootstrap_orders": [],
        "buy_orders": [
            {
                "side": "BUY",
                "price": 1.04,
                "qty": 100.0,
                "notional": 104.0,
                "role": "best_quote_reduce_short",
            }
        ],
        "sell_orders": [
            {
                "side": "SELL",
                "price": 0.96,
                "qty": 100.0,
                "notional": 96.0,
                "role": "best_quote_reduce_long",
            }
        ],
    }
    result = apply_take_profit_profit_guard(
        plan=plan,
        current_long_qty=100.0,
        current_short_qty=100.0,
        current_long_avg_price=1.02,
        current_short_avg_price=0.98,
        current_long_notional=102.0,
        current_short_notional=98.0,
        pause_long_position_notional=500.0,
        pause_short_position_notional=500.0,
        min_profit_ratio=0.001,
        tick_size=0.001,
        bid_price=0.999,
        ask_price=1.000,
        extra_long_guard_roles=long_roles,
        extra_short_guard_roles=short_roles,
    )
    return plan, result


def test_invalid_lease_keeps_both_losing_reduce_orders_guarded() -> None:
    plan, result = _apply_loss_guard(frozenset())

    assert result["adjusted_sell_orders"] == 1
    assert result["adjusted_buy_orders"] == 1
    assert plan["sell_orders"][0]["price"] > 1.02
    assert plan["buy_orders"][0]["price"] < 0.98


def test_valid_sell_lease_exempts_long_reduce_but_not_short_reduce() -> None:
    plan, result = _apply_loss_guard(frozenset({"best_quote_reduce_long"}))

    assert result["adjusted_sell_orders"] == 0
    assert result["adjusted_buy_orders"] == 1
    assert plan["sell_orders"][0]["price"] == pytest.approx(0.96)
    assert plan["buy_orders"][0]["price"] < 0.98


def test_submit_boundary_rechecks_expired_lease_before_order_post() -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    order = {
        "side": "SELL",
        "price": 0.96,
        "qty": 100.0,
        "notional": 96.0,
        "role": "best_quote_reduce_long",
        "position_side": "LONG",
    }
    plan_report = {
        "current_long_avg_price": 1.02,
        "current_short_avg_price": 0.0,
        "take_profit_min_profit_ratio": 0.001,
        "temporary_loss_recovery_authorization": _authorization_binding(active),
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        authorized, authorized_reason = _guard_temporary_loss_order_before_submit(
            args=args,
            order=order,
            plan_report=plan_report,
            strategy_mode="hedge_best_quote_maker_volume_v1",
            live_bid_price=0.999,
            live_ask_price=1.000,
            tick_size=0.001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
            now=NOW + timedelta(seconds=2),
        )
        expired, expired_reason = _guard_temporary_loss_order_before_submit(
            args=args,
            order=order,
            plan_report=plan_report,
            strategy_mode="hedge_best_quote_maker_volume_v1",
            live_bid_price=0.999,
            live_ask_price=1.000,
            tick_size=0.001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
            now=NOW + timedelta(seconds=60),
        )

    assert authorized is not None
    assert authorized_reason is None
    expected_prefix = temporary_loss_client_order_prefix(active)
    assert authorized["_temporary_loss_client_order_prefix"] == expected_prefix
    client_order_id = _build_temporary_loss_client_order_id(
        prefix=expected_prefix,
        order_index=1,
        nonce_ns=123456789,
    )
    assert client_order_id.startswith(expected_prefix)
    assert len(client_order_id) <= 36
    assert expired is None
    assert expired_reason["reason"] == "temporary_loss_lease_not_authorized"


def test_submit_boundary_rejects_plan_from_previous_recovery_generation() -> None:
    active = _active_loss_state(side=Side.SELL)
    advanced = replace(active, generation=active.generation + 1)
    args = _args(advanced)
    order = {
        "side": "SELL",
        "price": 0.96,
        "qty": 100.0,
        "notional": 96.0,
        "role": "best_quote_reduce_long",
        "position_side": "LONG",
        "reduce_only_no_loss_guard": (
            "bypassed_allow_loss_role_best_quote_reduce_long"
        ),
    }
    plan_report = {
        "current_long_avg_price": 1.02,
        "current_short_avg_price": 0.0,
        "take_profit_min_profit_ratio": 0.001,
        "temporary_loss_recovery_authorization": _authorization_binding(active),
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=advanced,
    ):
        guarded, reason = _guard_temporary_loss_order_before_submit(
            args=args,
            order=order,
            plan_report=plan_report,
            strategy_mode="hedge_best_quote_maker_volume_v1",
            live_bid_price=0.999,
            live_ask_price=1.000,
            tick_size=0.001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
            now=NOW + timedelta(seconds=2),
        )

    assert guarded is None
    assert reason["reason"] == "temporary_loss_lease_not_authorized"


def _receipt_events(state_path) -> list[dict[str, object]]:
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    journal = payload[TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY]
    assert journal["schema"] == "temporary_loss_runner_receipts_v1"
    return journal["events"]


def test_settling_lease_writes_one_idempotent_fenced_activation_receipt(
    tmp_path,
) -> None:
    settling, _active = _loss_states(side=Side.SELL)
    args = _args(settling)
    args.best_quote_maker_volume_allow_loss_reduce_only = False
    state_path = tmp_path / "runner-state.json"
    state_path.write_text(json.dumps({"sentinel": "kept"}), encoding="utf-8")
    args.state_path = str(state_path)

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=settling,
    ):
        first = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(milliseconds=100),
        )
        second = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(milliseconds=200),
        )

    assert first is not None
    assert first["action"] == "temporary_loss_activation_receipt"
    assert first["skip_normal_cycle"] is True
    assert second is not None
    assert second["receipt_id"] == first["receipt_id"]
    events = _receipt_events(state_path)
    assert len(events) == 1
    event = events[0]
    assert event["kind"] == "activation"
    assert event["symbol"] == "ARXUSDT"
    assert event["generation"] == settling.generation
    assert event["decision_id"] == settling.decision_id
    assert event["profile_digest"] == settling.desired_profile.digest
    assert event["action_lease_epoch"] == settling.action_lease.epoch
    assert event["side"] == "SELL"
    assert event["role"] == "best_quote_reduce_long"
    assert event["allow_loss_effective"] is False
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["sentinel"] == "kept"


@pytest.mark.parametrize(
    "terminal_state",
    [
        {"futures_terminal_drain": {"schema": "futures_terminal_drain_runtime_v1"}},
        {
            "futures_terminal_intent": {
                "schema": "futures_lifecycle_intent_v2",
                "status": "pending",
            }
        },
    ],
    ids=("terminal_owner", "resumable_terminal_intent"),
)
def test_terminal_owner_preempts_nonexpired_settling_activation_receipt(
    tmp_path,
    terminal_state: dict[str, object],
) -> None:
    settling, _active = _loss_states(side=Side.SELL)
    args = _args(settling)
    args.best_quote_maker_volume_allow_loss_reduce_only = False
    state_path = tmp_path / "runner-state.json"
    state_path.write_text(json.dumps(terminal_state), encoding="utf-8")

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=settling,
    ):
        maintenance = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(milliseconds=100),
        )

    assert maintenance is None
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY not in persisted


def test_expired_lease_reclaim_preempts_existing_terminal_intent(tmp_path) -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text(
        json.dumps(
            {
                "futures_terminal_intent": {
                    "schema": "futures_lifecycle_intent_v2",
                    "status": "pending",
                }
            }
        ),
        encoding="utf-8",
    )
    assert active.action_lease is not None

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        maintenance = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=active.action_lease.hard_expires_at,
        )

    assert maintenance is not None
    assert maintenance["action"] == "temporary_loss_expiry_cleanup_receipt"
    assert [event["kind"] for event in _receipt_events(state_path)] == [
        "expiry",
        "cleanup_needed",
    ]


def test_restoring_profile_writes_one_idempotent_applied_receipt(tmp_path) -> None:
    restoring = _restoring_loss_state(side=Side.SELL)
    args = _args(restoring)
    args.best_quote_maker_volume_allow_loss_reduce_only = False
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=restoring,
    ):
        first = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=4),
        )
        second = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=5),
        )

    assert first is not None
    assert first["action"] == "temporary_loss_restoration_applied_receipt"
    assert first["skip_normal_cycle"] is False
    assert second is not None
    assert second["receipt_id"] == first["receipt_id"]
    events = _receipt_events(state_path)
    assert len(events) == 1
    event = events[0]
    assert event == {
        "receipt_id": first["receipt_id"],
        "kind": "restoration_applied",
        "symbol": restoring.symbol,
        "generation": restoring.generation,
        "decision_id": restoring.decision_id,
        "profile_digest": restoring.desired_profile.digest,
        "observed_at": (NOW + timedelta(seconds=4)).isoformat(),
        "observation_seq": 0,
        "allow_loss_effective": False,
        "config_applied": True,
    }


def test_restoration_receipt_waits_for_matching_protected_profile(tmp_path) -> None:
    restoring = _restoring_loss_state(side=Side.BUY)
    args = _args(restoring)
    args.best_quote_maker_volume_allow_loss_reduce_only = False
    args.volatility_entry_pause_enabled = False
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=restoring,
    ):
        maintenance = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=4),
        )

    assert maintenance is not None
    assert maintenance["action"] == "temporary_loss_restoration_blocked"
    assert maintenance["skip_normal_cycle"] is True
    assert TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_expired_active_lease_is_disabled_and_writes_cleanup_receipts_once(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    args.state_path = str(state_path)
    assert active.action_lease is not None
    expired_at = active.action_lease.hard_expires_at

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        first = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=expired_at,
        )
        second = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=expired_at + timedelta(seconds=1),
        )

    assert first is not None
    assert first["action"] == "temporary_loss_expiry_cleanup_receipt"
    assert first["skip_normal_cycle"] is True
    assert first["allow_loss_effective"] is False
    assert second is not None
    assert args.best_quote_maker_volume_allow_loss_reduce_only is False
    events = _receipt_events(state_path)
    assert [event["kind"] for event in events] == ["expiry", "cleanup_needed"]
    for event in events:
        assert event["symbol"] == "ARXUSDT"
        assert event["generation"] == active.generation
        assert event["decision_id"] == active.decision_id
        assert event["profile_digest"] == active.desired_profile.digest
        assert event["action_lease_epoch"] == active.action_lease.epoch
        assert event["side"] == "SELL"
        assert event["role"] == "best_quote_reduce_long"
        assert event["allow_loss_effective"] is False
    assert events[1]["exchange_observation_required"] is True


def test_accepted_typed_lease_order_writes_idempotent_progress_receipt(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.BUY)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    args.state_path = str(state_path)
    authorization = _authorization_binding(active)
    client_order_id = _temporary_loss_client_order_id(active)
    plan_report = {"temporary_loss_recovery_authorization": authorization}
    submit_report = {
        "placed_orders": [
            {
                "request": {
                    "role": "best_quote_reduce_short",
                    "side": "BUY",
                    "time_in_force": "GTX",
                    "qty": 12.0,
                },
                "response": {
                    "orderId": 701,
                    "clientOrderId": client_order_id,
                    "status": "NEW",
                },
            }
        ]
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        first = _record_temporary_loss_recovery_progress(
            args,
            state_path=state_path,
            plan_report=plan_report,
            submit_report=submit_report,
            now=NOW + timedelta(seconds=2),
        )
        second = _record_temporary_loss_recovery_progress(
            args,
            state_path=state_path,
            plan_report=plan_report,
            submit_report=submit_report,
            now=NOW + timedelta(seconds=3),
        )

    assert first is not None
    assert first["action"] == "temporary_loss_progress_receipt"
    assert second is not None
    assert second["receipt_id"] == first["receipt_id"]
    events = _receipt_events(state_path)
    assert len(events) == 1
    progress = events[0]
    assert progress["kind"] == "progress"
    assert progress["symbol"] == "ARXUSDT"
    assert progress["generation"] == active.generation
    assert progress["decision_id"] == active.decision_id
    assert progress["profile_digest"] == active.desired_profile.digest
    assert progress["action_lease_epoch"] == active.action_lease.epoch
    assert progress["side"] == "BUY"
    assert progress["role"] == "best_quote_reduce_short"
    assert progress["observation_seq"] == 0
    assert progress["accepted_order_ids"] == ["701"]
    assert progress["accepted_client_order_ids"] == [client_order_id]
    assert progress["allow_loss_effective"] is True


@pytest.mark.parametrize(
    "request_patch",
    [
        {"role": "best_quote_reduce_long"},
        {"side": "SELL"},
        {"time_in_force": "GTC"},
    ],
    ids=("wrong_role", "wrong_side", "not_gtx"),
)
def test_wrong_scope_or_non_gtx_order_never_produces_lease_progress_receipt(
    tmp_path,
    request_patch: dict[str, object],
) -> None:
    active = _active_loss_state(side=Side.BUY)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    args.state_path = str(state_path)
    request = {
        "role": "best_quote_reduce_short",
        "side": "BUY",
        "time_in_force": "GTX",
        "qty": 12.0,
    }
    request.update(request_patch)
    submit_report = {
        "placed_orders": [
            {
                "request": request,
                "response": {
                    "orderId": 701,
                    "clientOrderId": "typed-loss-701",
                    "status": "NEW",
                },
            }
        ]
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        receipt = _record_temporary_loss_recovery_progress(
            args,
            state_path=state_path,
            plan_report={
                "temporary_loss_recovery_authorization": _authorization_binding(
                    active
                )
            },
            submit_report=submit_report,
            now=NOW + timedelta(seconds=2),
        )

    assert receipt is None
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY not in payload


def test_temporary_loss_progress_rejects_non_decision_client_order_id(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.BUY)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    args.state_path = str(state_path)

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        with pytest.raises(RuntimeError, match="outside its decision prefix"):
            _record_temporary_loss_recovery_progress(
                args,
                state_path=state_path,
                plan_report={
                    "temporary_loss_recovery_authorization": _authorization_binding(
                        active
                    )
                },
                submit_report={
                    "placed_orders": [
                        {
                            "request": {
                                "role": "best_quote_reduce_short",
                                "side": "BUY",
                                "time_in_force": "GTX",
                                "qty": 12.0,
                            },
                            "response": {
                                "orderId": 702,
                                "clientOrderId": "manual-same-side-gtx",
                                "status": "NEW",
                            },
                        }
                    ]
                },
                now=NOW + timedelta(seconds=2),
            )

    assert TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_expiry_maintenance_blocks_a_second_recovery_action_in_same_cycle(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")
    args.state_path = str(state_path)
    assert active.action_lease is not None

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        maintenance = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=active.action_lease.hard_expires_at,
        )

    assert maintenance is not None
    assert maintenance["skip_normal_cycle"] is True
    assert maintenance["recovery_action_count"] == 1


def test_stale_generation_yields_cycle_without_writing_a_false_receipt(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    args.recovery_generation -= 1
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        maintenance = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=2),
        )

    assert maintenance is not None
    assert maintenance["action"] == "temporary_loss_generation_fence_wait"
    assert maintenance["skip_normal_cycle"] is True
    assert maintenance["allow_loss_effective"] is False
    assert args.best_quote_maker_volume_allow_loss_reduce_only is False
    assert TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY not in json.loads(
        state_path.read_text(encoding="utf-8")
    )


def test_active_lease_with_unsafe_runtime_profile_yields_fail_closed_cycle(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.BUY)
    args = _args(active)
    args.volatility_entry_pause_enabled = False
    state_path = tmp_path / "runner-state.json"
    state_path.write_text("{}", encoding="utf-8")

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        maintenance = _maintain_temporary_loss_recovery_receipts(
            args,
            state_path=state_path,
            now=NOW + timedelta(seconds=2),
        )

    assert maintenance is not None
    assert maintenance["action"] == "temporary_loss_profile_fence_wait"
    assert maintenance["skip_normal_cycle"] is True
    assert maintenance["allow_loss_effective"] is False
    assert args.best_quote_maker_volume_allow_loss_reduce_only is False


def test_typed_loss_lease_rejects_aggressive_non_gtx_submit() -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    order = {
        "side": "SELL",
        "price": 0.96,
        "qty": 100.0,
        "notional": 96.0,
        "role": "best_quote_reduce_long",
        "position_side": "LONG",
        "execution_type": "aggressive",
    }

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        guarded, reason = _guard_temporary_loss_order_before_submit(
            args=args,
            order=order,
            plan_report={
                "temporary_loss_recovery_authorization": _authorization_binding(
                    active
                )
            },
            strategy_mode="hedge_best_quote_maker_volume_v1",
            live_bid_price=0.999,
            live_ask_price=1.000,
            tick_size=0.001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
            now=NOW + timedelta(seconds=2),
        )

    assert guarded is None
    assert reason == {
        "reason": "temporary_loss_requires_gtx",
        "role": "best_quote_reduce_long",
    }


def test_submit_boundary_yields_to_terminal_owner(tmp_path) -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _args(active)
    state_path = tmp_path / "runner-state.json"
    state_path.write_text(
        json.dumps({"futures_terminal_drain": {"decision_id": "terminal-1"}}),
        encoding="utf-8",
    )
    args.state_path = str(state_path)

    with patch(
        "grid_optimizer.loop_runner.JsonRecoveryStore.read",
        return_value=active,
    ):
        guarded, reason = _guard_temporary_loss_order_before_submit(
            args=args,
            order={
                "side": "SELL",
                "price": 0.96,
                "qty": 100.0,
                "notional": 96.0,
                "role": "best_quote_reduce_long",
                "position_side": "LONG",
            },
            plan_report={
                "temporary_loss_recovery_authorization": _authorization_binding(
                    active
                )
            },
            strategy_mode="hedge_best_quote_maker_volume_v1",
            live_bid_price=0.999,
            live_ask_price=1.000,
            tick_size=0.001,
            min_qty=1.0,
            min_notional=5.0,
            step_size=1.0,
            now=NOW + timedelta(seconds=2),
        )

    assert guarded is None
    assert reason == {
        "reason": "terminal_owner_preempts_temporary_loss",
        "role": "best_quote_reduce_long",
    }
    assert args.best_quote_maker_volume_allow_loss_reduce_only is False


def test_main_reclaims_expired_lease_before_a_failing_target_or_audit_query(
    tmp_path,
) -> None:
    active = _active_loss_state(side=Side.SELL)
    args = _build_parser().parse_args([])
    args.symbol = active.symbol
    args.iterations = 1
    args.recovery_control_path = str(tmp_path / "recovery-control.json")
    args.recovery_generation = active.generation
    args.best_quote_maker_volume_allow_loss_reduce_only = True
    args.best_quote_maker_volume_net_loss_reduce_enabled = False
    args.hard_loss_forced_reduce_enabled = False
    args.volatility_entry_pause_enabled = True
    args.volatility_entry_pause_1m_abs_return_ratio = 0.001
    args.anti_chase_entry_guard_enabled = True
    args.state_path = str(tmp_path / "runner-state.json")
    args.plan_json = str(tmp_path / "plan.json")
    args.submit_report_json = str(tmp_path / "submit.json")
    args.summary_jsonl = str(tmp_path / "summary.jsonl")
    Path(args.state_path).write_text("{}", encoding="utf-8")
    parser = Mock()
    parser.parse_args.return_value = args

    with (
        patch("grid_optimizer.loop_runner._build_parser", return_value=parser),
        patch("grid_optimizer.loop_runner.FuturesMarketStream"),
        patch(
            "grid_optimizer.loop_runner._wait_for_runner_market_stream_snapshot",
            return_value=None,
        ),
        patch(
            "grid_optimizer.loop_runner._maybe_start_runner_user_data_stream",
            return_value=None,
        ),
        patch(
            "grid_optimizer.loop_runner.JsonRecoveryStore.read",
            return_value=active,
        ),
        patch(
            "grid_optimizer.loop_runner._sync_account_audit",
            side_effect=AssertionError("audit must run after lease expiry"),
        ) as audit_sync,
        patch(
            "grid_optimizer.loop_runner._maybe_handle_runtime_guard",
            side_effect=AssertionError("target query must run after lease expiry"),
        ) as target_query,
        patch("grid_optimizer.loop_runner._append_jsonl"),
        patch("grid_optimizer.loop_runner._print_cycle_summary"),
        patch("grid_optimizer.loop_runner.time.sleep"),
    ):
        main()

    audit_sync.assert_not_called()
    target_query.assert_not_called()
    assert args.best_quote_maker_volume_allow_loss_reduce_only is False
    assert [event["kind"] for event in _receipt_events(Path(args.state_path))] == [
        "expiry",
        "cleanup_needed",
    ]
