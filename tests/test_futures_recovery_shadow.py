from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    OrderRole,
    RecoveryPhase,
    RecoveryState,
    Side,
)
from grid_optimizer.futures_recovery_shadow import (
    flow_blockers_from_legacy,
    plan_legacy_shadow,
)


NOW = datetime(2026, 7, 15, 2, 0, tzinfo=timezone.utc)
BASELINE = {
    "best_quote_maker_volume_allow_loss_reduce_only": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
    "volatility_entry_pause_enabled": True,
}


def test_recent_submit_partial_drift_routes_missing_leg_not_runner_restart() -> None:
    assessment = {
        "low_volume": True,
        "ineffective_orders": True,
        "planned_entry_buy_order_count": 1,
        "planned_entry_sell_order_count": 0,
    }
    blockers = flow_blockers_from_legacy(
        assessment=assessment,
        preflight_results=(
            {
                "action": "hold_arx_exchange_order_drift_after_recent_activity",
                "allow_recovery_check": True,
            },
        ),
    )

    assert blockers.runner_faults == ()
    assert blockers.missing_entry_sides == (Side.SELL,)

    plan = plan_legacy_shadow(
        symbol="ARXUSDT",
        captured_at=NOW,
        baseline=BASELINE,
        assessment=assessment,
        preflight_results=(
            {
                "action": "hold_arx_exchange_order_drift_after_recent_activity",
                "allow_recovery_check": True,
            },
        ),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        round_id="shadow-partial-1",
    )

    assert plan.action_id is ActionId.MAKER_FLOW_RECOVER
    assert plan.side is Side.SELL
    assert plan.order_role is OrderRole.ENTRY


def test_quiet_exchange_drift_preempts_maker_recovery_once() -> None:
    plan = plan_legacy_shadow(
        symbol="ARXUSDT",
        captured_at=NOW,
        baseline=BASELINE,
        assessment={
            "low_volume": True,
            "ineffective_orders": True,
            "planned_entry_buy_order_count": 1,
            "planned_entry_sell_order_count": 0,
        },
        preflight_results=({"action": "restart_arx_exchange_order_drift"},),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        round_id="shadow-quiet-1",
    )

    assert plan.action_id is ActionId.RUNNER_RECOVER
    assert plan.suppressed_actions == (ActionId.MAKER_FLOW_RECOVER,)


def test_soft_inventory_maps_to_directional_normal_reduce_not_loss_relief() -> None:
    assessment = {
        "low_volume": True,
        "ineffective_orders": True,
        "long_near_soft": True,
        "short_near_soft": False,
        "planned_entry_buy_order_count": 0,
        "planned_entry_sell_order_count": 0,
        "cost_gate_blocked": True,
        "frozen_total_notional": 0.0,
    }
    plan = plan_legacy_shadow(
        symbol="OUSDT",
        captured_at=NOW,
        baseline=BASELINE,
        assessment=assessment,
        preflight_results=(),
        state=RecoveryState.initial("OUSDT", BASELINE, now=NOW),
        round_id="shadow-soft-1",
    )

    assert plan.action_id is ActionId.INVENTORY_RECOVER
    assert plan.side is Side.SELL
    assert plan.order_role is OrderRole.REDUCE_ONLY
    assert (
        plan.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )


def test_explicit_loss_only_requires_exhausted_directional_inventory_attempt() -> None:
    assessment = {
        "low_volume": True,
        "ineffective_orders": True,
        "long_near_soft": True,
        "loss_blocked_reduce_fallback_sides": ["SELL"],
    }
    state = RecoveryState.initial("OUSDT", BASELINE, now=NOW).with_exhausted_attempt(
        ActionId.INVENTORY_RECOVER,
        Side.SELL,
        OrderRole.REDUCE_ONLY,
        now=NOW,
    )

    plan = plan_legacy_shadow(
        symbol="OUSDT",
        captured_at=NOW,
        baseline=BASELINE,
        assessment=assessment,
        preflight_results=(),
        state=state,
        round_id="shadow-loss-1",
    )

    assert plan.action_id is ActionId.TEMPORARY_LOSS_RELIEF
    assert plan.side is Side.SELL
    assert plan.next_state.phase is RecoveryPhase.SETTLING
    assert (
        plan.desired_profile.fields["best_quote_maker_volume_allow_loss_reduce_only"]
        is False
    )
    assert plan.desired_profile.fields["recovery_action_lease_active"] is False


def test_shadow_mapping_is_pure_and_does_not_mutate_legacy_inputs() -> None:
    assessment = {
        "low_volume": True,
        "volatility_entry_pause_active": True,
        "reasons": ["low_volume"],
        "planned_entry_buy_order_count": 0,
        "planned_entry_sell_order_count": 0,
    }
    preflight = [{"action": "hold_arx_exchange_order_drift_after_recent_activity"}]
    original_assessment = deepcopy(assessment)
    original_preflight = deepcopy(preflight)

    blockers = flow_blockers_from_legacy(
        assessment=assessment,
        preflight_results=preflight,
        observation_fingerprint="plan-42:submit-42",
    )

    assert assessment == original_assessment
    assert preflight == original_preflight
    assert blockers.safety_reasons == ("volatility_entry_pause",)
    assert blockers.safety_evidence_fingerprint == "plan-42:submit-42"


def test_safety_shadow_maps_typed_observation_without_fabricating_freshness() -> None:
    assessment = {
        "volatility_entry_pause_active": True,
        "safety_observation_seq": 42,
        "safety_observed_at": NOW.isoformat(),
    }

    blockers = flow_blockers_from_legacy(
        assessment=assessment,
        observation_fingerprint="safety-plan-42",
    )

    assert blockers.safety_observation_seq == 42
    assert blockers.safety_observed_at == NOW

    plan = plan_legacy_shadow(
        symbol="ARXUSDT",
        captured_at=NOW,
        baseline=BASELINE,
        assessment=assessment,
        preflight_results=(),
        state=RecoveryState.initial("ARXUSDT", BASELINE, now=NOW),
        round_id="shadow-safety-1",
        observation_fingerprint="safety-plan-42",
    )

    assert plan.action_id is ActionId.SAFETY_CONVERGE
    assert plan.next_state.phase is RecoveryPhase.ACTIVE
    assert plan.next_state.safety_lease is not None
    assert plan.next_state.safety_lease.observation_seq == 42
