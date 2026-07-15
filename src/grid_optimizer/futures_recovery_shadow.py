"""Pure legacy-to-coordinator shadow adapter.

The adapter consumes values that the legacy guard already calculated.  It
does not read files, call Binance, write control state, or execute lifecycle
effects.  This keeps shadow comparison observational until a symbol performs
an explicit ownership cutover.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

from .futures_recovery_coordinator import (
    ActionId,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    RecoveryState,
    RoundPlan,
    Side,
    SymbolSnapshot,
    materialize_profile,
)


_RECENT_ACTIVITY_DRIFT_HOLD = "hold_arx_exchange_order_drift_after_recent_activity"


def _truthy(value: Any) -> bool:
    return bool(value)


def _stable_sides(values: Sequence[Side]) -> tuple[Side, ...]:
    return tuple(sorted(set(values), key=lambda side: side.value))


def _parse_sides(values: Any) -> tuple[Side, ...]:
    if not isinstance(values, (list, tuple, set, frozenset)):
        return ()
    parsed: list[Side] = []
    for value in values:
        try:
            parsed.append(Side(str(value).upper().strip()))
        except ValueError:
            continue
    return _stable_sides(parsed)


def _fallback_safety_fingerprint(assessment: Mapping[str, Any]) -> str:
    payload = {
        "volatility_entry_pause_active": _truthy(
            assessment.get("volatility_entry_pause_active")
        ),
        "volatility_inventory_gate_active": _truthy(
            assessment.get("volatility_inventory_gate_active")
        ),
        "pause_reasons": sorted(
            str(item) for item in (assessment.get("pause_reasons") or [])
        ),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_observed_at(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _preflight_runner_faults(
    preflight_results: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    faults: list[str] = []
    for result in preflight_results:
        action = str(result.get("action") or "").strip()
        if not action or action == _RECENT_ACTIVITY_DRIFT_HOLD:
            continue
        if (
            action.startswith("restart_")
            or action.startswith("recover_runner_inactive")
            or action.startswith("restart_runner_inactive")
            or "runner_error" in action
            or "effective_control_drift" in action
            or "two_sided_control_drift" in action
            or "exchange_order_drift_restart_cooldown" in action
        ):
            faults.append(action)
    return tuple(dict.fromkeys(faults))


def flow_blockers_from_legacy(
    *,
    assessment: Mapping[str, Any],
    preflight_results: Sequence[Mapping[str, Any]] = (),
    observation_fingerprint: str | None = None,
    safety_observation_seq: int | None = None,
    safety_observed_at: datetime | None = None,
    episode_fingerprint: str | None = None,
) -> FlowBlockerAssessment:
    """Map one already-captured legacy assessment into closed blocker codes."""

    safety_reasons: list[str] = []
    if _truthy(assessment.get("volatility_entry_pause_active")):
        safety_reasons.append("volatility_entry_pause")
    if _truthy(assessment.get("required_safety_observation_unavailable")):
        safety_reasons.append("required_safety_observation_unavailable")

    unknown_order_ownership = _truthy(
        assessment.get("unknown_order_ownership")
    ) or _truthy(assessment.get("ledger_position_drift_blocked"))
    runner_faults = list(_preflight_runner_faults(preflight_results))
    if _truthy(assessment.get("runner_inactive")):
        runner_faults.append("runner_inactive")
    if _truthy(assessment.get("generation_mismatch")):
        runner_faults.append("generation_mismatch")

    low_or_ineffective = _truthy(assessment.get("low_volume")) or _truthy(
        assessment.get("ineffective_orders")
    )
    inventory_sides: list[Side] = []
    if low_or_ineffective and not unknown_order_ownership:
        if _truthy(assessment.get("long_near_soft")) or _truthy(
            assessment.get("long_near_cap")
        ):
            inventory_sides.append(Side.SELL)
        if _truthy(assessment.get("short_near_soft")) or _truthy(
            assessment.get("short_near_cap")
        ):
            inventory_sides.append(Side.BUY)

    missing_entry_sides: list[Side] = []
    if low_or_ineffective and not unknown_order_ownership:
        if (
            "planned_entry_buy_order_count" in assessment
            and int(assessment.get("planned_entry_buy_order_count") or 0) <= 0
        ):
            missing_entry_sides.append(Side.BUY)
        if (
            "planned_entry_sell_order_count" in assessment
            and int(assessment.get("planned_entry_sell_order_count") or 0) <= 0
        ):
            missing_entry_sides.append(Side.SELL)

    inventory_tuple = _stable_sides(inventory_sides)
    explicit_loss_sides = _parse_sides(
        assessment.get("loss_blocked_reduce_fallback_sides")
    )
    loss_only_sides = tuple(
        side for side in explicit_loss_sides if side in inventory_tuple
    )

    frozen_required = _truthy(assessment.get("frozen_ledger_repair_required"))
    frozen_side_value = assessment.get("frozen_repair_side")
    try:
        frozen_side = (
            Side(str(frozen_side_value).upper().strip())
            if frozen_side_value is not None
            else None
        )
    except ValueError:
        frozen_side = None

    fingerprint = None
    if safety_reasons:
        fingerprint = observation_fingerprint or _fallback_safety_fingerprint(
            assessment
        )
        if safety_observation_seq is None:
            raw_seq = assessment.get("safety_observation_seq")
            if type(raw_seq) is int and raw_seq >= 0:
                safety_observation_seq = raw_seq
        if safety_observed_at is None:
            safety_observed_at = _parse_observed_at(
                assessment.get("safety_observed_at")
            )

    return FlowBlockerAssessment(
        terminal_reason=(
            str(assessment.get("terminal_reason"))
            if assessment.get("terminal_reason")
            else None
        ),
        safety_reasons=tuple(dict.fromkeys(safety_reasons)),
        safety_evidence_fingerprint=fingerprint,
        safety_observation_seq=safety_observation_seq,
        safety_observed_at=safety_observed_at,
        runner_faults=tuple(dict.fromkeys(runner_faults)),
        unknown_order_ownership=unknown_order_ownership,
        frozen_ledger_repair_required=frozen_required,
        frozen_repair_side=frozen_side,
        inventory_reduce_sides=inventory_tuple,
        loss_only_blocked_sides=loss_only_sides,
        missing_entry_sides=_stable_sides(missing_entry_sides),
        baseline_rebase_requested=_truthy(assessment.get("baseline_rebase_requested")),
        baseline_tune_sides=_parse_sides(assessment.get("baseline_tune_sides")),
        episode_fingerprint=(
            episode_fingerprint
            or (
                str(assessment.get("recovery_episode_fingerprint"))
                if assessment.get("recovery_episode_fingerprint")
                else None
            )
        ),
    )


def plan_legacy_shadow(
    *,
    symbol: str,
    captured_at: datetime,
    baseline: Mapping[str, Any],
    assessment: Mapping[str, Any],
    preflight_results: Sequence[Mapping[str, Any]],
    state: RecoveryState,
    round_id: str,
    observation_fingerprint: str | None = None,
    safety_observation_seq: int | None = None,
    safety_observed_at: datetime | None = None,
    episode_fingerprint: str | None = None,
    engine: FuturesRecoveryDecisionEngine | None = None,
) -> RoundPlan:
    """Return a pure shadow plan without committing or executing it."""

    expected_baseline = materialize_profile(
        baseline=baseline,
        action_id=ActionId.NOOP,
    )
    if expected_baseline.digest != state.baseline_profile.digest:
        raise ValueError("shadow baseline does not match recovery state")
    blockers = flow_blockers_from_legacy(
        assessment=assessment,
        preflight_results=preflight_results,
        observation_fingerprint=observation_fingerprint,
        safety_observation_seq=safety_observation_seq,
        safety_observed_at=safety_observed_at,
        episode_fingerprint=episode_fingerprint,
    )
    snapshot = SymbolSnapshot(
        symbol=symbol.upper().strip(),
        captured_at=captured_at,
        assessment=blockers,
    )
    return (engine or FuturesRecoveryDecisionEngine()).plan_round(
        snapshot=snapshot,
        state=state,
        now=captured_at,
        round_id=round_id,
    )
