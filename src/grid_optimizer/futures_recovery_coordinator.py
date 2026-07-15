"""Pure, symbol-scoped recovery decision core.

This module deliberately has no filesystem, subprocess, or exchange imports.
It owns the closed action priority, complete-profile materialization, bounded
temporary leases, and the rule that one symbol round produces one decision.
Adapters may persist a :class:`RecoveryState` and execute the single returned
effect, but they may not add another action after planning.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol


JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


class ActionId(str, Enum):
    TERMINAL_STOP = "terminal_stop"
    SAFETY_CONVERGE = "safety_converge"
    RUNNER_RECOVER = "runner_recover"
    FROZEN_LEDGER_REPAIR = "frozen_ledger_repair"
    INVENTORY_RECOVER = "inventory_recover"
    TEMPORARY_LOSS_RELIEF = "temporary_loss_relief"
    MAKER_FLOW_RECOVER = "maker_flow_recover"
    BASELINE_REBASE = "baseline_rebase"
    BASELINE_TUNE = "baseline_tune"
    NOOP = "noop"


ACTION_PRIORITY = (
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
_ACTION_RANK = {action_id: rank for rank, action_id in enumerate(ACTION_PRIORITY)}


class ActionMode(str, Enum):
    ENTER = "enter"
    HOLD = "hold"
    ADVANCE = "advance"
    EXIT = "exit"


class EffectStage(str, Enum):
    NONE = "none"
    RUNNER_STOP = "runner_stop"
    RUNNER_START = "runner_start"
    RUNNER_RESTART = "runner_restart"
    MANAGED_GTX_CANCEL = "managed_gtx_cancel"
    LOCAL_STATE_REPAIR = "local_state_repair"


class RecoveryPhase(str, Enum):
    STABLE = "stable"
    SETTLING = "settling"
    ACTIVE = "active"
    CLEANING = "cleaning"
    RESTORING = "restoring"
    COOLDOWN = "cooldown"
    STOP_PENDING = "stop_pending"
    STOPPED = "stopped"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderRole(str, Enum):
    ENTRY = "entry"
    REDUCE_ONLY = "reduce_only"
    FROZEN_HEDGE_ADJUST = "frozen_hedge_adjust"


class LedgerClass(str, Enum):
    NORMAL_BQ = "normal_bq"
    FROZEN_BQ = "frozen_bq"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ExecutionPolicy:
    order_type: str = "LIMIT"
    time_in_force: str = "GTX"
    post_only: bool = True


@dataclass(frozen=True)
class ManagedProfile:
    fields: Mapping[str, Any]
    digest: str
    execution_policy: ExecutionPolicy = field(default_factory=ExecutionPolicy)


def _canonical_digest(payload: Mapping[str, JsonValue]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _thaw_json(value: Any) -> JsonValue:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"unsupported managed JSON value: {type(value).__name__}")


def _freeze_json(value: JsonValue) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def materialize_profile(
    *,
    baseline: Mapping[str, JsonValue],
    action_id: ActionId,
    action_updates: Mapping[str, JsonValue] | None = None,
    action_lease: ActionLease | None = None,
    action_lease_active: bool = False,
    safety_lease: SafetyLease | None = None,
) -> ManagedProfile:
    """Build the complete desired profile exactly once.

    The live control document is intentionally not an input.  Temporary
    overlays therefore cannot leak into a future baseline, and protected
    fields are applied once at the final boundary instead of being toggled by
    later branches.
    """

    fields = {str(key): _thaw_json(value) for key, value in baseline.items()}
    fields.update(
        {str(key): _thaw_json(value) for key, value in (action_updates or {}).items()}
    )
    if action_id is ActionId.TEMPORARY_LOSS_RELIEF:
        if action_lease is None:
            raise ValueError("temporary loss relief requires a typed action lease")
        if action_lease.action_id is not ActionId.TEMPORARY_LOSS_RELIEF:
            raise ValueError("temporary loss relief lease action mismatch")
        if action_lease.order_role is not OrderRole.REDUCE_ONLY:
            raise ValueError("temporary loss relief lease must be reduce-only")
        fields.update(
            {
                "recovery_action_lease_epoch": action_lease.epoch,
                "recovery_action_lease_side": action_lease.side.value,
                "recovery_action_lease_order_role": action_lease.order_role.value,
                "recovery_action_lease_hard_expires_at": action_lease.hard_expires_at.isoformat(),
                "recovery_action_lease_active": bool(action_lease_active),
            }
        )
    elif action_lease is not None or action_lease_active:
        raise ValueError("only temporary loss relief may carry an action lease")
    if action_id is ActionId.SAFETY_CONVERGE:
        if safety_lease is None:
            raise ValueError("safety converge requires a typed safety lease")
        fields.update(
            {
                "recovery_safety_lease_epoch": safety_lease.epoch,
                "recovery_safety_lease_hard_expires_at": safety_lease.hard_expires_at.isoformat(),
                "recovery_safety_evidence_fingerprint": safety_lease.evidence_fingerprint,
                "recovery_safety_observation_seq": safety_lease.observation_seq,
                "recovery_safety_observed_at": safety_lease.observed_at.isoformat(),
            }
        )
    elif safety_lease is not None:
        raise ValueError("only safety converge may carry a safety lease")
    fields["best_quote_maker_volume_allow_loss_reduce_only"] = bool(
        action_id is ActionId.TEMPORARY_LOSS_RELIEF
        and action_lease is not None
        and action_lease_active
    )
    fields["best_quote_maker_volume_net_loss_reduce_enabled"] = False
    fields["hard_loss_forced_reduce_enabled"] = False
    fields["volatility_entry_pause_enabled"] = True
    digest_payload: dict[str, JsonValue] = dict(fields)
    digest_payload["_recovery_execution_policy"] = {
        "order_type": "LIMIT",
        "time_in_force": "GTX",
        "post_only": True,
    }
    frozen = MappingProxyType(
        {key: _freeze_json(value) for key, value in fields.items()}
    )
    return ManagedProfile(fields=frozen, digest=_canonical_digest(digest_payload))


@dataclass(frozen=True, order=True)
class FlowRoleKey:
    ledger_class: LedgerClass
    side: Side
    order_role: OrderRole


@dataclass(frozen=True)
class FlowClock:
    quote_missing_since: datetime | None = None
    submit_missing_since: datetime | None = None
    fill_missing_since: datetime | None = None


@dataclass(frozen=True)
class ActionAttempt:
    action_id: ActionId
    side: Side
    order_role: OrderRole
    exhausted_at: datetime


@dataclass(frozen=True)
class ActionLease:
    epoch: int
    action_id: ActionId
    side: Side
    order_role: OrderRole
    started_at: datetime
    hard_expires_at: datetime
    activated_at: datetime | None = None


@dataclass(frozen=True)
class SafetyLease:
    epoch: int
    started_at: datetime
    hard_expires_at: datetime
    evidence_fingerprint: str
    observation_seq: int
    observed_at: datetime


@dataclass(frozen=True)
class CleanupObligation:
    source_action_id: ActionId
    source_decision_id: str
    action_lease_epoch: int
    open_order_ids: tuple[str, ...]
    needs_manifest_rebuild: bool
    created_at: datetime
    attempt_count: int
    next_retry_at: datetime


@dataclass(frozen=True)
class CleanupProof:
    source_decision_id: str
    action_lease_epoch: int
    remaining_order_ids: tuple[str, ...]
    user_trades_watermark: int
    observed_at: datetime


@dataclass(frozen=True)
class ActivationReceipt:
    decision_id: str
    generation: int
    profile_digest: str
    action_lease_epoch: int
    observed_at: datetime


@dataclass(frozen=True)
class EffectReceipt:
    decision_id: str
    stage: EffectStage
    effect_epoch: int
    observed_at: datetime


@dataclass(frozen=True)
class EffectCommand:
    decision_id: str
    stage: EffectStage
    effect_epoch: int


@dataclass(frozen=True)
class ProgressReceipt:
    decision_id: str
    generation: int
    side: Side | None
    order_role: OrderRole | None
    observation_seq: int
    observed_at: datetime


@dataclass(frozen=True)
class FlowBlockerAssessment:
    terminal_reason: str | None = None
    safety_reasons: tuple[str, ...] = ()
    safety_evidence_fingerprint: str | None = None
    safety_observation_seq: int | None = None
    safety_observed_at: datetime | None = None
    runner_faults: tuple[str, ...] = ()
    unknown_order_ownership: bool = False
    frozen_ledger_repair_required: bool = False
    frozen_repair_side: Side | None = None
    inventory_reduce_sides: tuple[Side, ...] = ()
    loss_only_blocked_sides: tuple[Side, ...] = ()
    missing_entry_sides: tuple[Side, ...] = ()
    baseline_rebase_requested: bool = False
    baseline_tune_sides: tuple[Side, ...] = ()
    episode_fingerprint: str | None = None


@dataclass(frozen=True)
class SymbolSnapshot:
    symbol: str
    captured_at: datetime
    assessment: FlowBlockerAssessment
    applied_generation: int | None = None
    applied_profile_digest: str | None = None
    progress_receipts: tuple[ProgressReceipt, ...] = ()
    cleanup_proof: CleanupProof | None = None
    action_lease_open_order_ids: tuple[str, ...] = ()
    exchange_observation_available: bool = True
    effect_receipt: EffectReceipt | None = None
    activation_receipt: ActivationReceipt | None = None


@dataclass(frozen=True)
class RecoveryPolicy:
    action_progress_timeout: timedelta = timedelta(minutes=2)
    action_hard_ttl: timedelta = timedelta(minutes=5)
    safety_hard_ttl: timedelta = timedelta(minutes=3)
    cooldown: timedelta = timedelta(minutes=1)
    restore_timeout: timedelta = timedelta(minutes=2)
    cleanup_retry_interval: timedelta = timedelta(seconds=15)
    max_temporary_loss_leases_per_episode: int = 2
    max_safety_observation_age: timedelta = timedelta(seconds=30)
    terminal_reason_allowlist: frozenset[str] = frozenset(
        {
            "competition_target_confirmed",
            "trusted_competition_window_end",
            "unrecoverable_managed_document_corruption",
        }
    )
    round_history_size: int = 64

    def __post_init__(self) -> None:
        for name in (
            "action_progress_timeout",
            "action_hard_ttl",
            "safety_hard_ttl",
            "cooldown",
            "restore_timeout",
            "cleanup_retry_interval",
            "max_safety_observation_age",
        ):
            if getattr(self, name).total_seconds() < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.action_hard_ttl < self.action_progress_timeout:
            raise ValueError("action_hard_ttl must cover action_progress_timeout")
        if self.max_temporary_loss_leases_per_episode < 1:
            raise ValueError("max_temporary_loss_leases_per_episode must be positive")
        if self.round_history_size < 1:
            raise ValueError("round_history_size must be positive")


@dataclass(frozen=True)
class RecoveryState:
    symbol: str
    document_revision: int
    generation: int
    baseline_profile: ManagedProfile
    desired_profile: ManagedProfile
    desired_runner_state: str
    phase: RecoveryPhase
    active_action: ActionId
    decision_id: str | None
    side: Side | None
    order_role: OrderRole | None
    reasons: tuple[str, ...]
    issued_at: datetime | None
    progress_deadline_at: datetime | None
    hard_expires_at: datetime | None
    cooldown_until: datetime | None
    action_lease: ActionLease | None
    safety_lease: SafetyLease | None
    cleanup_obligation: CleanupObligation | None
    exhausted_attempts: tuple[ActionAttempt, ...]
    flow_clocks: Mapping[FlowRoleKey, FlowClock]
    action_lease_epoch: int
    safety_lease_epoch: int
    temporary_loss_lease_uses: Mapping[str, int]
    active_episode_fingerprint: str | None
    cleanup_successor_action: ActionId | None
    post_cleanup_effect_stage: EffectStage
    terminal_stop_confirmed: bool
    effect_epoch: int
    pending_effect_stage: EffectStage
    pending_effect_epoch: int | None
    recent_round_ids: tuple[str, ...]
    last_round_id: str | None
    fairness_last_selected_sides: Mapping[str, Side] = field(
        default_factory=lambda: MappingProxyType({})
    )

    @classmethod
    def initial(
        cls,
        symbol: str,
        baseline: Mapping[str, JsonValue],
        *,
        now: datetime,
    ) -> "RecoveryState":
        normalized = symbol.upper().strip()
        profile = materialize_profile(
            baseline=baseline,
            action_id=ActionId.NOOP,
        )
        return cls(
            symbol=normalized,
            document_revision=0,
            generation=0,
            baseline_profile=profile,
            desired_profile=profile,
            desired_runner_state="running",
            phase=RecoveryPhase.STABLE,
            active_action=ActionId.NOOP,
            decision_id=None,
            side=None,
            order_role=None,
            reasons=(),
            issued_at=None,
            progress_deadline_at=None,
            hard_expires_at=None,
            cooldown_until=None,
            action_lease=None,
            safety_lease=None,
            cleanup_obligation=None,
            exhausted_attempts=(),
            flow_clocks=MappingProxyType({}),
            action_lease_epoch=0,
            safety_lease_epoch=0,
            temporary_loss_lease_uses=MappingProxyType({}),
            active_episode_fingerprint=None,
            cleanup_successor_action=None,
            post_cleanup_effect_stage=EffectStage.NONE,
            terminal_stop_confirmed=False,
            effect_epoch=0,
            pending_effect_stage=EffectStage.NONE,
            pending_effect_epoch=None,
            recent_round_ids=(),
            last_round_id=None,
            fairness_last_selected_sides=MappingProxyType({}),
        )

    def with_exhausted_attempt(
        self,
        action_id: ActionId,
        side: Side,
        order_role: OrderRole,
        *,
        now: datetime,
    ) -> "RecoveryState":
        attempts = [
            item
            for item in self.exhausted_attempts
            if (item.action_id, item.side, item.order_role)
            != (action_id, side, order_role)
        ]
        attempts.append(ActionAttempt(action_id, side, order_role, now))
        attempts.sort(
            key=lambda item: (
                _ACTION_RANK[item.action_id],
                item.side.value,
                item.order_role.value,
            )
        )
        return replace(self, exhausted_attempts=tuple(attempts))

    def with_flow_clock(
        self,
        key: FlowRoleKey,
        *,
        quote_missing_since: datetime | None,
        submit_missing_since: datetime | None,
        fill_missing_since: datetime | None,
    ) -> "RecoveryState":
        clocks = dict(self.flow_clocks)
        clocks[key] = FlowClock(
            quote_missing_since=quote_missing_since,
            submit_missing_since=submit_missing_since,
            fill_missing_since=fill_missing_since,
        )
        return replace(self, flow_clocks=MappingProxyType(clocks))

    def with_temporary_loss_lease_use(
        self,
        episode_fingerprint: str,
        *,
        count: int,
    ) -> "RecoveryState":
        if not episode_fingerprint:
            raise ValueError("episode_fingerprint is required")
        if count < 0:
            raise ValueError("temporary loss lease count cannot be negative")
        uses = dict(self.temporary_loss_lease_uses)
        uses[episode_fingerprint] = count
        return replace(self, temporary_loss_lease_uses=MappingProxyType(uses))

    def attempt_exhausted(
        self,
        action_id: ActionId,
        side: Side,
        order_role: OrderRole,
    ) -> bool:
        return any(
            (item.action_id, item.side, item.order_role)
            == (action_id, side, order_role)
            for item in self.exhausted_attempts
        )


def temporary_loss_authorized(
    state: RecoveryState,
    *,
    now: datetime,
) -> bool:
    """Fail-closed runner gate independent of coordinator scheduling.

    A runner may call this before planning and again immediately before submit;
    persisted ``allow_loss=true`` alone is never sufficient authorization.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    lease = state.action_lease
    desired = state.desired_profile.fields
    return bool(
        state.phase is RecoveryPhase.ACTIVE
        and state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
        and lease is not None
        and lease.action_id is ActionId.TEMPORARY_LOSS_RELIEF
        and lease.order_role is OrderRole.REDUCE_ONLY
        and lease.activated_at is not None
        and lease.activated_at <= now < lease.hard_expires_at
        and state.action_lease_epoch == lease.epoch
        and state.side is lease.side
        and state.order_role is lease.order_role
        and desired.get("best_quote_maker_volume_allow_loss_reduce_only") is True
        and desired.get("recovery_action_lease_active") is True
        and desired.get("recovery_action_lease_epoch") == lease.epoch
        and desired.get("recovery_action_lease_side") == lease.side.value
        and desired.get("recovery_action_lease_order_role") == lease.order_role.value
        and desired.get("recovery_action_lease_hard_expires_at")
        == lease.hard_expires_at.isoformat()
        and state.desired_profile.execution_policy == ExecutionPolicy()
    )


@dataclass(frozen=True)
class RoundPlan:
    symbol: str
    round_id: str
    action_id: ActionId
    mode: ActionMode
    side: Side | None
    order_role: OrderRole | None
    reasons: tuple[str, ...]
    suppressed_actions: tuple[ActionId, ...]
    desired_profile: ManagedProfile
    desired_runner_state: str
    effect_stage: EffectStage
    effect_epoch: int | None
    liveness_status: str
    next_state: RecoveryState
    effect_error: str | None = None


@dataclass(frozen=True)
class _Candidate:
    action_id: ActionId
    side: Side | None = None
    order_role: OrderRole | None = None
    reasons: tuple[str, ...] = ()
    updates: Mapping[str, JsonValue] = field(default_factory=dict)
    effect_stage: EffectStage = EffectStage.NONE
    runner_state: str = "running"


@dataclass(frozen=True)
class _ActionEvaluation:
    candidates: tuple[_Candidate, ...] = ()
    blocked_reasons: tuple[str, ...] = ()


ActionEvaluator = Callable[
    [FlowBlockerAssessment, RecoveryState, RecoveryPolicy],
    _ActionEvaluation,
]


@dataclass(frozen=True)
class ActionDefinition:
    """One registration point for an action's admission and hold predicate."""

    action_id: ActionId
    evaluate: ActionEvaluator


def _stable_sides(sides: tuple[Side, ...]) -> tuple[Side, ...]:
    return tuple(sorted(set(sides), key=lambda side: side.value))


def _directional_fairness_key(
    action_id: ActionId,
    order_role: OrderRole,
) -> str:
    return f"{action_id.value}:{order_role.value}"


def _episode_fingerprint(assessment: FlowBlockerAssessment) -> str:
    if assessment.episode_fingerprint:
        return assessment.episode_fingerprint
    payload = {
        "inventory": [
            side.value
            for side in sorted(
                set(assessment.inventory_reduce_sides),
                key=lambda item: item.value,
            )
        ],
        "loss": [
            side.value
            for side in sorted(
                set(assessment.loss_only_blocked_sides),
                key=lambda item: item.value,
            )
        ],
        "missing_entry": [
            side.value
            for side in sorted(
                set(assessment.missing_entry_sides),
                key=lambda item: item.value,
            )
        ],
    }
    return _canonical_digest(payload)


def _directional_candidate(
    action_id: ActionId,
    side: Side,
    role: OrderRole,
    reason: str,
) -> _Candidate:
    return _Candidate(
        action_id=action_id,
        side=side,
        order_role=role,
        reasons=(reason,),
        updates={
            "recovery_order_side": side.value,
            "recovery_order_role": role.value,
            "recovery_order_ledger_class": LedgerClass.NORMAL_BQ.value,
        },
    )


def _evaluate_terminal(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    policy: RecoveryPolicy,
) -> _ActionEvaluation:
    if not assessment.terminal_reason:
        return _ActionEvaluation()
    if assessment.terminal_reason not in policy.terminal_reason_allowlist:
        return _ActionEvaluation(
            blocked_reasons=(
                f"unregistered_terminal_reason:{assessment.terminal_reason}",
            )
        )
    return _ActionEvaluation(
        candidates=(
            _Candidate(
                ActionId.TERMINAL_STOP,
                reasons=(assessment.terminal_reason,),
                effect_stage=EffectStage.RUNNER_STOP,
                runner_state="stopped",
            ),
        )
    )


def _evaluate_safety(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    if not assessment.safety_reasons:
        return _ActionEvaluation()
    return _ActionEvaluation(
        candidates=(
            _Candidate(
                ActionId.SAFETY_CONVERGE,
                reasons=tuple(assessment.safety_reasons),
                updates={
                    "recovery_entry_blocked_sides": ["BUY", "SELL"],
                    "recovery_reduce_only_allowed_sides": ["BUY", "SELL"],
                },
            ),
        )
    )


def _evaluate_runner(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    reasons = list(assessment.runner_faults)
    if assessment.unknown_order_ownership:
        reasons.append("unknown_order_ownership")
    if not reasons:
        return _ActionEvaluation()
    return _ActionEvaluation(
        candidates=(
            _Candidate(
                ActionId.RUNNER_RECOVER,
                reasons=tuple(dict.fromkeys(reasons)),
                effect_stage=EffectStage.RUNNER_RESTART,
            ),
        )
    )


def _evaluate_frozen_ledger(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    if not assessment.frozen_ledger_repair_required:
        return _ActionEvaluation()
    return _ActionEvaluation(
        candidates=(
            _Candidate(
                ActionId.FROZEN_LEDGER_REPAIR,
                side=assessment.frozen_repair_side,
                order_role=OrderRole.FROZEN_HEDGE_ADJUST,
                reasons=("frozen_ledger_delta",),
                updates={
                    "recovery_order_role": OrderRole.FROZEN_HEDGE_ADJUST.value,
                    "recovery_order_ledger_class": LedgerClass.FROZEN_BQ.value,
                },
            ),
        )
    )


def _evaluate_inventory(
    assessment: FlowBlockerAssessment,
    state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    candidates = []
    for side in _stable_sides(assessment.inventory_reduce_sides):
        exhausted = state.attempt_exhausted(
            ActionId.INVENTORY_RECOVER,
            side,
            OrderRole.REDUCE_ONLY,
        )
        if side in assessment.loss_only_blocked_sides and exhausted:
            continue
        candidates.append(
            _directional_candidate(
                ActionId.INVENTORY_RECOVER,
                side,
                OrderRole.REDUCE_ONLY,
                "normal_inventory_soft_or_cap",
            )
        )
    return _ActionEvaluation(candidates=tuple(candidates))


def _evaluate_temporary_loss(
    assessment: FlowBlockerAssessment,
    state: RecoveryState,
    policy: RecoveryPolicy,
) -> _ActionEvaluation:
    candidates = []
    blocked_reasons = []
    episode_fingerprint = _episode_fingerprint(assessment)
    for side in _stable_sides(assessment.inventory_reduce_sides):
        if (
            side not in assessment.loss_only_blocked_sides
            or not state.attempt_exhausted(
                ActionId.INVENTORY_RECOVER,
                side,
                OrderRole.REDUCE_ONLY,
            )
        ):
            continue
        already_active = (
            state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
            and state.side is side
            and state.action_lease is not None
        )
        if (
            not already_active
            and state.temporary_loss_lease_uses.get(episode_fingerprint, 0)
            >= policy.max_temporary_loss_leases_per_episode
        ):
            blocked_reasons.append(
                f"temporary_loss_episode_budget_exhausted:{side.value}"
            )
            continue
        candidates.append(
            _directional_candidate(
                ActionId.TEMPORARY_LOSS_RELIEF,
                side,
                OrderRole.REDUCE_ONLY,
                "inventory_recovery_exhausted_loss_only",
            )
        )
    return _ActionEvaluation(
        candidates=tuple(candidates),
        blocked_reasons=tuple(blocked_reasons),
    )


def _evaluate_maker_flow(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    return _ActionEvaluation(
        candidates=tuple(
            _directional_candidate(
                ActionId.MAKER_FLOW_RECOVER,
                side,
                OrderRole.ENTRY,
                "missing_entry_role",
            )
            for side in _stable_sides(assessment.missing_entry_sides)
        )
    )


def _evaluate_baseline_rebase(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    if not assessment.baseline_rebase_requested:
        return _ActionEvaluation()
    return _ActionEvaluation(
        candidates=(
            _Candidate(
                ActionId.BASELINE_REBASE,
                reasons=("baseline_change_requested",),
            ),
        )
    )


def _evaluate_baseline_tune(
    assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    return _ActionEvaluation(
        candidates=tuple(
            _directional_candidate(
                ActionId.BASELINE_TUNE,
                side,
                OrderRole.ENTRY,
                "registered_baseline_capacity_insufficient",
            )
            for side in _stable_sides(assessment.baseline_tune_sides)
        )
    )


def _evaluate_noop(
    _assessment: FlowBlockerAssessment,
    _state: RecoveryState,
    _policy: RecoveryPolicy,
) -> _ActionEvaluation:
    return _ActionEvaluation()


ACTION_DEFINITIONS = (
    ActionDefinition(ActionId.TERMINAL_STOP, _evaluate_terminal),
    ActionDefinition(ActionId.SAFETY_CONVERGE, _evaluate_safety),
    ActionDefinition(ActionId.RUNNER_RECOVER, _evaluate_runner),
    ActionDefinition(ActionId.FROZEN_LEDGER_REPAIR, _evaluate_frozen_ledger),
    ActionDefinition(ActionId.INVENTORY_RECOVER, _evaluate_inventory),
    ActionDefinition(ActionId.TEMPORARY_LOSS_RELIEF, _evaluate_temporary_loss),
    ActionDefinition(ActionId.MAKER_FLOW_RECOVER, _evaluate_maker_flow),
    ActionDefinition(ActionId.BASELINE_REBASE, _evaluate_baseline_rebase),
    ActionDefinition(ActionId.BASELINE_TUNE, _evaluate_baseline_tune),
    ActionDefinition(ActionId.NOOP, _evaluate_noop),
)
if tuple(item.action_id for item in ACTION_DEFINITIONS) != ACTION_PRIORITY:
    raise RuntimeError("action definitions must match the closed priority order")


class FuturesRecoveryDecisionEngine:
    def __init__(self, *, policy: RecoveryPolicy | None = None) -> None:
        self.policy = policy or RecoveryPolicy()

    def plan_round(
        self,
        *,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        plan = self._plan_round(
            snapshot=snapshot,
            state=state,
            now=now,
            round_id=round_id,
        )
        history = tuple(dict.fromkeys((*state.recent_round_ids, round_id)))[
            -self.policy.round_history_size :
        ]
        next_state = replace(
            plan.next_state,
            recent_round_ids=history,
        )
        return replace(plan, next_state=next_state)

    def _plan_round(
        self,
        *,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        if snapshot.symbol.upper().strip() != state.symbol:
            raise ValueError("snapshot symbol does not match recovery state")
        if not round_id:
            raise ValueError("round_id is required")

        if state.phase in {
            RecoveryPhase.SETTLING,
            RecoveryPhase.ACTIVE,
            RecoveryPhase.CLEANING,
            RecoveryPhase.RESTORING,
            RecoveryPhase.COOLDOWN,
        }:
            preemption = self._plan_higher_priority_preemption(
                snapshot=snapshot,
                state=state,
                now=now,
                round_id=round_id,
            )
            if preemption is not None:
                return preemption
        if state.phase is RecoveryPhase.CLEANING:
            return self._plan_cleaning(snapshot, state, now, round_id)
        if state.phase is RecoveryPhase.SETTLING:
            return self._plan_settling(snapshot, state, now, round_id)
        if state.phase is RecoveryPhase.RESTORING:
            return self._plan_restoring(snapshot, state, now, round_id)
        if state.phase is RecoveryPhase.COOLDOWN:
            return self._plan_cooldown(snapshot, state, now, round_id)
        if state.phase is RecoveryPhase.STOP_PENDING:
            return self._plan_stop_pending(snapshot, state, round_id)
        if state.phase is RecoveryPhase.STOPPED:
            return self._hold(state, round_id, liveness_status="terminal")
        if state.phase is RecoveryPhase.ACTIVE:
            return self._plan_active(snapshot, state, now, round_id)
        return self._plan_stable(snapshot, state, now, round_id)

    def _plan_higher_priority_preemption(
        self,
        *,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan | None:
        candidates = self._candidates(snapshot.assessment, state)
        selected = self._select_candidate(candidates, state)
        same_safety_reentry = bool(
            selected.action_id is ActionId.SAFETY_CONVERGE
            and state.active_action is ActionId.SAFETY_CONVERGE
            and state.phase
            in {
                RecoveryPhase.RESTORING,
                RecoveryPhase.COOLDOWN,
            }
            and state.safety_lease is not None
            and self._safety_evidence_is_fresh_successor(
                snapshot,
                state.safety_lease,
            )
        )
        if (
            _ACTION_RANK[selected.action_id] >= _ACTION_RANK[state.active_action]
            and not same_safety_reentry
        ):
            return None
        suppressed = tuple(
            action_id
            for action_id in ACTION_PRIORITY
            if action_id not in {selected.action_id, ActionId.NOOP}
            and (
                any(item.action_id is action_id for item in candidates)
                or action_id is state.active_action
            )
        )
        if (
            (
                state.action_lease is not None
                and state.action_lease.activated_at is not None
            )
            or state.cleanup_obligation is not None
            or bool(snapshot.action_lease_open_order_ids)
        ):
            return self._enter_candidate_through_cleanup(
                snapshot=snapshot,
                state=state,
                selected=selected,
                suppressed=suppressed,
                now=now,
                round_id=round_id,
            )
        return self._enter_candidate(
            snapshot=snapshot,
            state=state,
            selected=selected,
            suppressed=suppressed,
            now=now,
            round_id=round_id,
        )

    def _plan_stable(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        candidates = self._candidates(snapshot.assessment, state)
        selected = self._select_candidate(candidates, state)
        suppressed = tuple(
            action_id
            for action_id in ACTION_PRIORITY
            if action_id not in {selected.action_id, ActionId.NOOP}
            and any(item.action_id is action_id for item in candidates)
        )
        if selected.action_id is ActionId.NOOP:
            episode_cleared = self._episode_blockers_cleared(snapshot.assessment)
            next_state = replace(
                state,
                document_revision=state.document_revision + 1,
                exhausted_attempts=(
                    () if episode_cleared else state.exhausted_attempts
                ),
                temporary_loss_lease_uses=(
                    MappingProxyType({})
                    if episode_cleared
                    else state.temporary_loss_lease_uses
                ),
                active_episode_fingerprint=(
                    None if episode_cleared else state.active_episode_fingerprint
                ),
                last_round_id=round_id,
            )
            return RoundPlan(
                symbol=state.symbol,
                round_id=round_id,
                action_id=ActionId.NOOP,
                mode=ActionMode.HOLD,
                side=None,
                order_role=None,
                reasons=selected.reasons,
                suppressed_actions=(),
                desired_profile=state.desired_profile,
                desired_runner_state=state.desired_runner_state,
                effect_stage=EffectStage.NONE,
                effect_epoch=None,
                liveness_status="blocked" if selected.reasons else "healthy",
                next_state=next_state,
            )

        return self._enter_candidate(
            snapshot=snapshot,
            state=state,
            selected=selected,
            suppressed=suppressed,
            now=now,
            round_id=round_id,
        )

    def _enter_candidate(
        self,
        *,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        selected: _Candidate,
        suppressed: tuple[ActionId, ...],
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        action_lease = None
        action_epoch = state.action_lease_epoch
        temporary_loss_uses = dict(state.temporary_loss_lease_uses)
        episode_fingerprint = _episode_fingerprint(snapshot.assessment)
        safety_lease = None
        safety_epoch = state.safety_lease_epoch
        hard_expires_at = now + self.policy.action_hard_ttl
        if selected.action_id is ActionId.TEMPORARY_LOSS_RELIEF:
            action_epoch += 1
            if selected.side is None or selected.order_role is None:
                raise ValueError("temporary loss relief requires side and role")
            action_lease = ActionLease(
                epoch=action_epoch,
                action_id=selected.action_id,
                side=selected.side,
                order_role=selected.order_role,
                started_at=now,
                hard_expires_at=hard_expires_at,
            )
        if selected.action_id is ActionId.SAFETY_CONVERGE:
            safety_epoch += 1
            hard_expires_at = now + self.policy.safety_hard_ttl
            safety_lease = self._new_safety_lease(
                snapshot=snapshot,
                epoch=safety_epoch,
                now=now,
                hard_expires_at=hard_expires_at,
            )
        profile = materialize_profile(
            baseline=state.baseline_profile.fields,
            action_id=selected.action_id,
            action_updates=selected.updates,
            action_lease=action_lease,
            action_lease_active=False,
            safety_lease=safety_lease,
        )
        effect_epoch = state.effect_epoch
        pending_effect_epoch = None
        if selected.effect_stage is not EffectStage.NONE:
            effect_epoch += 1
            pending_effect_epoch = effect_epoch
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            generation=state.generation + 1,
            desired_profile=profile,
            desired_runner_state=selected.runner_state,
            phase=(
                RecoveryPhase.STOP_PENDING
                if selected.action_id is ActionId.TERMINAL_STOP
                else (
                    RecoveryPhase.SETTLING
                    if selected.action_id is ActionId.TEMPORARY_LOSS_RELIEF
                    else RecoveryPhase.ACTIVE
                )
            ),
            active_action=selected.action_id,
            decision_id=f"{state.symbol}:{round_id}",
            side=selected.side,
            order_role=selected.order_role,
            reasons=selected.reasons,
            issued_at=now,
            progress_deadline_at=now + self.policy.action_progress_timeout,
            hard_expires_at=hard_expires_at,
            cooldown_until=None,
            action_lease=action_lease,
            safety_lease=safety_lease,
            cleanup_obligation=None,
            action_lease_epoch=action_epoch,
            safety_lease_epoch=safety_epoch,
            temporary_loss_lease_uses=MappingProxyType(temporary_loss_uses),
            active_episode_fingerprint=episode_fingerprint,
            cleanup_successor_action=None,
            post_cleanup_effect_stage=EffectStage.NONE,
            terminal_stop_confirmed=False,
            effect_epoch=effect_epoch,
            pending_effect_stage=selected.effect_stage,
            pending_effect_epoch=pending_effect_epoch,
            last_round_id=round_id,
            exhausted_attempts=self._exhausted_after_selection(
                state,
                selected,
            ),
            fairness_last_selected_sides=self._fairness_after_selection(
                state,
                selected,
            ),
        )
        return RoundPlan(
            symbol=state.symbol,
            round_id=round_id,
            action_id=selected.action_id,
            mode=ActionMode.ENTER,
            side=selected.side,
            order_role=selected.order_role,
            reasons=selected.reasons,
            suppressed_actions=suppressed,
            desired_profile=profile,
            desired_runner_state=selected.runner_state,
            effect_stage=selected.effect_stage,
            effect_epoch=pending_effect_epoch,
            liveness_status=(
                "terminal"
                if selected.action_id is ActionId.TERMINAL_STOP
                else "recovering"
            ),
            next_state=next_state,
        )

    def _new_safety_lease(
        self,
        *,
        snapshot: SymbolSnapshot,
        epoch: int,
        now: datetime,
        hard_expires_at: datetime,
    ) -> SafetyLease:
        fingerprint = snapshot.assessment.safety_evidence_fingerprint
        if not fingerprint:
            raise ValueError("safety action requires an evidence fingerprint")
        observed_at = snapshot.assessment.safety_observed_at
        observation_seq = snapshot.assessment.safety_observation_seq
        if observed_at is None or observation_seq is None:
            raise ValueError(
                "safety action requires typed observation time and sequence"
            )
        if (
            observed_at > snapshot.captured_at
            or snapshot.captured_at - observed_at
            > self.policy.max_safety_observation_age
        ):
            raise ValueError("safety evidence is not fresh for this snapshot")
        return SafetyLease(
            epoch=epoch,
            started_at=now,
            hard_expires_at=hard_expires_at,
            evidence_fingerprint=fingerprint,
            observation_seq=observation_seq,
            observed_at=observed_at,
        )

    def _enter_candidate_through_cleanup(
        self,
        *,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        selected: _Candidate,
        suppressed: tuple[ActionId, ...],
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        """Preempt without losing an activated lease cleanup obligation."""

        if selected.action_id is ActionId.TEMPORARY_LOSS_RELIEF:
            raise ValueError("a loss lease cannot preempt through another lease")
        obligation = state.cleanup_obligation
        cleanup_effect = EffectStage.NONE
        if obligation is None:
            lease = state.action_lease
            if (
                lease is None
                or state.decision_id is None
                or (
                    lease.activated_at is None
                    and not snapshot.action_lease_open_order_ids
                )
            ):
                raise ValueError(
                    "cleanup preemption requires an activated action lease"
                )
            open_order_ids = tuple(dict.fromkeys(snapshot.action_lease_open_order_ids))
            needs_manifest_rebuild = (
                not open_order_ids or not snapshot.exchange_observation_available
            )
            cleanup_effect = (
                EffectStage.LOCAL_STATE_REPAIR
                if needs_manifest_rebuild
                else EffectStage.MANAGED_GTX_CANCEL
            )
            cleanup_starts_now = selected.action_id is not ActionId.TERMINAL_STOP
            obligation = CleanupObligation(
                source_action_id=lease.action_id,
                source_decision_id=state.decision_id,
                action_lease_epoch=lease.epoch,
                open_order_ids=open_order_ids,
                needs_manifest_rebuild=needs_manifest_rebuild,
                created_at=now,
                attempt_count=1 if cleanup_starts_now else 0,
                next_retry_at=(
                    now + self.policy.cleanup_retry_interval
                    if cleanup_starts_now
                    else now
                ),
            )

        safety_lease = None
        safety_epoch = state.safety_lease_epoch
        hard_expires_at = now + self.policy.action_hard_ttl
        if selected.action_id is ActionId.SAFETY_CONVERGE:
            safety_epoch += 1
            hard_expires_at = now + self.policy.safety_hard_ttl
            safety_lease = self._new_safety_lease(
                snapshot=snapshot,
                epoch=safety_epoch,
                now=now,
                hard_expires_at=hard_expires_at,
            )
        profile = materialize_profile(
            baseline=state.baseline_profile.fields,
            action_id=selected.action_id,
            action_updates=selected.updates,
            safety_lease=safety_lease,
        )
        if selected.action_id is ActionId.TERMINAL_STOP:
            effect = EffectStage.RUNNER_STOP
            pending_effect = EffectStage.RUNNER_STOP
            post_cleanup_effect = EffectStage.NONE
            effect_epoch = state.effect_epoch + 1
            pending_effect_epoch = effect_epoch
        else:
            effect = cleanup_effect
            pending_effect = (
                effect if effect is not EffectStage.NONE else state.pending_effect_stage
            )
            post_cleanup_effect = selected.effect_stage
            if effect is not EffectStage.NONE:
                effect_epoch = state.effect_epoch + 1
                pending_effect_epoch = effect_epoch
            else:
                effect_epoch = state.effect_epoch
                pending_effect_epoch = state.pending_effect_epoch
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            generation=state.generation + 1,
            desired_profile=profile,
            desired_runner_state=selected.runner_state,
            phase=RecoveryPhase.CLEANING,
            active_action=selected.action_id,
            decision_id=f"{state.symbol}:{round_id}",
            side=selected.side,
            order_role=selected.order_role,
            reasons=selected.reasons,
            issued_at=now,
            progress_deadline_at=now + self.policy.action_progress_timeout,
            hard_expires_at=hard_expires_at,
            cooldown_until=None,
            safety_lease=safety_lease,
            cleanup_obligation=obligation,
            safety_lease_epoch=safety_epoch,
            active_episode_fingerprint=_episode_fingerprint(snapshot.assessment),
            cleanup_successor_action=selected.action_id,
            post_cleanup_effect_stage=post_cleanup_effect,
            terminal_stop_confirmed=False,
            effect_epoch=effect_epoch,
            pending_effect_stage=pending_effect,
            pending_effect_epoch=pending_effect_epoch,
            last_round_id=round_id,
            exhausted_attempts=self._exhausted_after_selection(
                state,
                selected,
            ),
            fairness_last_selected_sides=self._fairness_after_selection(
                state,
                selected,
            ),
        )
        return RoundPlan(
            symbol=state.symbol,
            round_id=round_id,
            action_id=selected.action_id,
            mode=ActionMode.ENTER,
            side=selected.side,
            order_role=selected.order_role,
            reasons=selected.reasons,
            suppressed_actions=suppressed,
            desired_profile=profile,
            desired_runner_state=selected.runner_state,
            effect_stage=effect,
            effect_epoch=(
                pending_effect_epoch if effect is not EffectStage.NONE else None
            ),
            liveness_status="recovering",
            next_state=next_state,
        )

    def _plan_settling(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        lease = state.action_lease
        if (
            state.active_action is not ActionId.TEMPORARY_LOSS_RELIEF
            or lease is None
            or lease.activated_at is not None
        ):
            raise ValueError("settling phase requires an unactivated loss lease")
        expired = bool(
            (
                state.progress_deadline_at is not None
                and now >= state.progress_deadline_at
            )
            or now >= lease.hard_expires_at
        )
        receipt = snapshot.activation_receipt
        receipt_matches = bool(
            receipt is not None
            and receipt.decision_id == state.decision_id
            and receipt.generation == state.generation
            and receipt.profile_digest == state.desired_profile.digest
            and receipt.action_lease_epoch == lease.epoch
            and state.issued_at is not None
            and receipt.observed_at >= state.issued_at
            and receipt.observed_at <= snapshot.captured_at
        )
        if expired:
            return self._exit_active(
                snapshot,
                state,
                now,
                round_id,
                reason="activation_deadline",
            )
        if receipt_matches:
            activated_lease = replace(lease, activated_at=now)
            profile = materialize_profile(
                baseline=state.baseline_profile.fields,
                action_id=ActionId.TEMPORARY_LOSS_RELIEF,
                action_updates=state.desired_profile.fields,
                action_lease=activated_lease,
                action_lease_active=True,
            )
            episode_fingerprint = (
                state.active_episode_fingerprint
                or _episode_fingerprint(snapshot.assessment)
            )
            uses = dict(state.temporary_loss_lease_uses)
            uses[episode_fingerprint] = uses.get(episode_fingerprint, 0) + 1
            next_state = replace(
                state,
                document_revision=state.document_revision + 1,
                generation=state.generation + 1,
                desired_profile=profile,
                phase=RecoveryPhase.ACTIVE,
                action_lease=activated_lease,
                temporary_loss_lease_uses=MappingProxyType(uses),
                pending_effect_stage=EffectStage.NONE,
                pending_effect_epoch=None,
                last_round_id=round_id,
            )
            return self._round_plan(
                next_state,
                round_id,
                mode=ActionMode.ADVANCE,
                effect_stage=EffectStage.NONE,
                liveness_status="recovering",
            )
        return self._hold(state, round_id, liveness_status="recovering")

    def _plan_active(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        expired = bool(
            (
                state.progress_deadline_at is not None
                and now >= state.progress_deadline_at
            )
            or (state.hard_expires_at is not None and now >= state.hard_expires_at)
            or (
                state.action_lease is not None
                and now >= state.action_lease.hard_expires_at
            )
        )
        if state.pending_effect_stage is not EffectStage.NONE:
            if expired:
                return self._exit_active(
                    snapshot,
                    state,
                    now,
                    round_id,
                    reason="effect_receipt_deadline",
                )
            if not self._effect_receipt_matches(snapshot, state):
                return self._retry_pending_effect(state, round_id)
            state = replace(
                state,
                pending_effect_stage=EffectStage.NONE,
                pending_effect_epoch=None,
            )
        if state.active_action is ActionId.SAFETY_CONVERGE:
            return self._plan_active_safety(snapshot, state, now, round_id)

        progressed = self._has_current_progress(snapshot, state)
        eligible = self._active_still_eligible(snapshot.assessment, state)
        if progressed or expired or not eligible:
            exhausted_state = state
            if (
                expired
                and not progressed
                and state.side is not None
                and state.order_role is not None
            ):
                exhausted_state = state.with_exhausted_attempt(
                    state.active_action,
                    state.side,
                    state.order_role,
                    now=now,
                )
            return self._exit_active(
                snapshot,
                exhausted_state,
                now,
                round_id,
                reason=(
                    "progress"
                    if progressed
                    else "deadline" if expired else "condition_cleared"
                ),
            )
        return self._hold(state, round_id, liveness_status="recovering")

    def _plan_stop_pending(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        round_id: str,
    ) -> RoundPlan:
        if not self._effect_receipt_matches(snapshot, state):
            return self._retry_pending_effect(state, round_id)
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            phase=RecoveryPhase.STOPPED,
            terminal_stop_confirmed=True,
            pending_effect_stage=EffectStage.NONE,
            pending_effect_epoch=None,
            last_round_id=round_id,
        )
        return self._round_plan(
            next_state,
            round_id,
            mode=ActionMode.ADVANCE,
            effect_stage=EffectStage.NONE,
            liveness_status="terminal",
        )

    def _retry_pending_effect(
        self,
        state: RecoveryState,
        round_id: str,
    ) -> RoundPlan:
        if (
            state.pending_effect_stage is EffectStage.NONE
            or state.pending_effect_epoch is None
        ):
            raise ValueError("cannot retry an empty effect stage")
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            last_round_id=round_id,
        )
        return self._round_plan(
            next_state,
            round_id,
            mode=ActionMode.ADVANCE,
            effect_stage=state.pending_effect_stage,
            liveness_status="recovering",
        )

    @staticmethod
    def _effect_receipt_matches(
        snapshot: SymbolSnapshot,
        state: RecoveryState,
    ) -> bool:
        receipt = snapshot.effect_receipt
        return bool(
            receipt is not None
            and state.decision_id is not None
            and state.pending_effect_stage is not EffectStage.NONE
            and state.pending_effect_epoch is not None
            and receipt.decision_id == state.decision_id
            and receipt.stage is state.pending_effect_stage
            and receipt.effect_epoch == state.pending_effect_epoch
            and receipt.observed_at <= snapshot.captured_at
        )

    def _plan_active_safety(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        assessment = snapshot.assessment
        if not assessment.safety_reasons:
            return self._exit_active(
                snapshot, state, now, round_id, reason="safety_cleared"
            )
        lease = state.safety_lease
        if lease is None:
            raise ValueError("active safety action has no safety lease")
        if now < lease.hard_expires_at:
            return self._hold(state, round_id, liveness_status="recovering")
        fingerprint = assessment.safety_evidence_fingerprint
        observed_at = assessment.safety_observed_at
        observation_seq = assessment.safety_observation_seq
        fresh_successor = bool(
            self._safety_evidence_is_fresh_successor(snapshot, lease)
        )
        if not fresh_successor:
            return self._hold(state, round_id, liveness_status="blocked")

        next_epoch = state.safety_lease_epoch + 1
        renewed = SafetyLease(
            epoch=next_epoch,
            started_at=now,
            hard_expires_at=now + self.policy.safety_hard_ttl,
            evidence_fingerprint=str(fingerprint),
            observation_seq=int(observation_seq),
            observed_at=observed_at,
        )
        profile = materialize_profile(
            baseline=state.baseline_profile.fields,
            action_id=ActionId.SAFETY_CONVERGE,
            action_updates=state.desired_profile.fields,
            safety_lease=renewed,
        )
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            generation=state.generation + 1,
            desired_profile=profile,
            reasons=tuple(assessment.safety_reasons),
            safety_lease=renewed,
            safety_lease_epoch=next_epoch,
            hard_expires_at=renewed.hard_expires_at,
            progress_deadline_at=now + self.policy.action_progress_timeout,
            last_round_id=round_id,
        )
        return self._round_plan(
            next_state,
            round_id,
            mode=ActionMode.ADVANCE,
            effect_stage=EffectStage.NONE,
            liveness_status="recovering",
        )

    def _safety_evidence_is_fresh_successor(
        self,
        snapshot: SymbolSnapshot,
        lease: SafetyLease,
    ) -> bool:
        assessment = snapshot.assessment
        return bool(
            assessment.safety_evidence_fingerprint
            and assessment.safety_evidence_fingerprint != lease.evidence_fingerprint
            and assessment.safety_observation_seq is not None
            and assessment.safety_observation_seq > lease.observation_seq
            and assessment.safety_observed_at is not None
            and assessment.safety_observed_at > lease.observed_at
            and assessment.safety_observed_at <= snapshot.captured_at
            and snapshot.captured_at - assessment.safety_observed_at
            <= self.policy.max_safety_observation_age
        )

    def _exit_active(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
        *,
        reason: str,
    ) -> RoundPlan:
        profile = materialize_profile(
            baseline=state.baseline_profile.fields,
            action_id=ActionId.NOOP,
        )
        open_order_ids = (
            tuple(dict.fromkeys(snapshot.action_lease_open_order_ids))
            if state.action_lease is not None
            else ()
        )
        cleanup = None
        phase = RecoveryPhase.RESTORING
        effect = EffectStage.NONE
        if state.action_lease is not None and (
            state.action_lease.activated_at is not None or open_order_ids
        ):
            if state.decision_id is None:
                raise ValueError("leased action has no decision_id")
            needs_manifest_rebuild = (
                not open_order_ids or not snapshot.exchange_observation_available
            )
            cleanup = CleanupObligation(
                source_action_id=state.active_action,
                source_decision_id=state.decision_id,
                action_lease_epoch=state.action_lease.epoch,
                open_order_ids=open_order_ids,
                needs_manifest_rebuild=needs_manifest_rebuild,
                created_at=now,
                attempt_count=1,
                next_retry_at=now + self.policy.cleanup_retry_interval,
            )
            phase = RecoveryPhase.CLEANING
            effect = (
                EffectStage.LOCAL_STATE_REPAIR
                if needs_manifest_rebuild
                else EffectStage.MANAGED_GTX_CANCEL
            )
        effect_epoch = state.effect_epoch
        pending_effect_epoch = None
        if effect is not EffectStage.NONE:
            effect_epoch += 1
            pending_effect_epoch = effect_epoch
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            generation=state.generation + 1,
            desired_profile=profile,
            desired_runner_state="running",
            phase=phase,
            action_lease=(state.action_lease if cleanup is not None else None),
            safety_lease=(
                state.safety_lease
                if state.active_action is ActionId.SAFETY_CONVERGE
                else None
            ),
            reasons=tuple(dict.fromkeys((*state.reasons, reason))),
            progress_deadline_at=now + self.policy.restore_timeout,
            cleanup_obligation=cleanup,
            cleanup_successor_action=None,
            post_cleanup_effect_stage=EffectStage.NONE,
            terminal_stop_confirmed=False,
            effect_epoch=effect_epoch,
            pending_effect_stage=effect,
            pending_effect_epoch=pending_effect_epoch,
            last_round_id=round_id,
        )
        return self._round_plan(
            next_state,
            round_id,
            mode=ActionMode.EXIT,
            effect_stage=effect,
            liveness_status="recovering",
        )

    def _plan_cleaning(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        obligation = state.cleanup_obligation
        if obligation is None:
            raise ValueError("cleaning phase has no cleanup obligation")
        if (
            state.cleanup_successor_action is ActionId.TERMINAL_STOP
            and not state.terminal_stop_confirmed
        ):
            if self._effect_receipt_matches(snapshot, state):
                next_state = replace(
                    state,
                    document_revision=state.document_revision + 1,
                    terminal_stop_confirmed=True,
                    pending_effect_stage=EffectStage.NONE,
                    pending_effect_epoch=None,
                    last_round_id=round_id,
                )
                return self._round_plan(
                    next_state,
                    round_id,
                    mode=ActionMode.ADVANCE,
                    effect_stage=EffectStage.NONE,
                    liveness_status="recovering",
                )
            if (
                state.pending_effect_stage is EffectStage.RUNNER_STOP
                and state.pending_effect_epoch is not None
            ):
                retry_state = state
            else:
                next_effect_epoch = state.effect_epoch + 1
                retry_state = replace(
                    state,
                    effect_epoch=next_effect_epoch,
                    pending_effect_stage=EffectStage.RUNNER_STOP,
                    pending_effect_epoch=next_effect_epoch,
                )
            return self._retry_pending_effect(retry_state, round_id)
        if self._cleanup_proof_valid(
            snapshot.cleanup_proof,
            obligation,
            captured_at=snapshot.captured_at,
        ):
            successor = state.cleanup_successor_action
            successor_expired = bool(
                successor not in {None, ActionId.TERMINAL_STOP}
                and (
                    (
                        state.progress_deadline_at is not None
                        and now >= state.progress_deadline_at
                    )
                    or (
                        state.hard_expires_at is not None
                        and now >= state.hard_expires_at
                    )
                )
            )
            desired_profile = state.desired_profile
            desired_runner_state = state.desired_runner_state
            generation = state.generation
            safety_lease = state.safety_lease
            safety_lease_epoch = state.safety_lease_epoch
            hard_expires_at = state.hard_expires_at
            reasons = state.reasons
            live_expired_safety = bool(
                successor_expired
                and successor is ActionId.SAFETY_CONVERGE
                and snapshot.assessment.safety_reasons
            )
            if live_expired_safety:
                if safety_lease is None:
                    raise ValueError("expired safety successor has no safety lease")
                phase = RecoveryPhase.ACTIVE
                effect = EffectStage.NONE
                desired_runner_state = "running"
                reasons = tuple(snapshot.assessment.safety_reasons)
                if self._safety_evidence_is_fresh_successor(
                    snapshot,
                    safety_lease,
                ):
                    safety_lease_epoch += 1
                    safety_lease = SafetyLease(
                        epoch=safety_lease_epoch,
                        started_at=now,
                        hard_expires_at=now + self.policy.safety_hard_ttl,
                        evidence_fingerprint=str(
                            snapshot.assessment.safety_evidence_fingerprint
                        ),
                        observation_seq=int(snapshot.assessment.safety_observation_seq),
                        observed_at=snapshot.assessment.safety_observed_at,
                    )
                    desired_profile = materialize_profile(
                        baseline=state.baseline_profile.fields,
                        action_id=ActionId.SAFETY_CONVERGE,
                        action_updates=state.desired_profile.fields,
                        safety_lease=safety_lease,
                    )
                    generation += 1
                    hard_expires_at = safety_lease.hard_expires_at
                    progress_deadline_at = now + self.policy.action_progress_timeout
                    liveness_status = "recovering"
                else:
                    progress_deadline_at = state.progress_deadline_at
                    liveness_status = "blocked"
            elif successor_expired:
                phase = RecoveryPhase.RESTORING
                effect = EffectStage.NONE
                progress_deadline_at = now + self.policy.restore_timeout
                liveness_status = "recovering"
                desired_profile = materialize_profile(
                    baseline=state.baseline_profile.fields,
                    action_id=ActionId.NOOP,
                )
                desired_runner_state = "running"
                generation += 1
                safety_lease = None
                reasons = tuple(
                    dict.fromkeys((*state.reasons, "successor_activation_deadline"))
                )
            elif successor is None:
                phase = RecoveryPhase.RESTORING
                effect = EffectStage.NONE
                progress_deadline_at = now + self.policy.restore_timeout
                liveness_status = "recovering"
            elif successor is ActionId.TERMINAL_STOP:
                if not state.terminal_stop_confirmed:
                    raise ValueError("terminal cleanup completed before runner stop")
                phase = RecoveryPhase.STOPPED
                effect = EffectStage.NONE
                progress_deadline_at = None
                liveness_status = "terminal"
            else:
                phase = RecoveryPhase.ACTIVE
                effect = state.post_cleanup_effect_stage
                progress_deadline_at = state.progress_deadline_at
                liveness_status = "recovering"
            effect_epoch = state.effect_epoch
            pending_effect_epoch = None
            if effect is not EffectStage.NONE:
                effect_epoch += 1
                pending_effect_epoch = effect_epoch
            next_state = replace(
                state,
                document_revision=state.document_revision + 1,
                generation=generation,
                desired_profile=desired_profile,
                desired_runner_state=desired_runner_state,
                phase=phase,
                action_lease=None,
                safety_lease=safety_lease,
                safety_lease_epoch=safety_lease_epoch,
                cleanup_obligation=None,
                reasons=reasons,
                progress_deadline_at=progress_deadline_at,
                hard_expires_at=hard_expires_at,
                cleanup_successor_action=None,
                post_cleanup_effect_stage=EffectStage.NONE,
                effect_epoch=effect_epoch,
                pending_effect_stage=effect,
                pending_effect_epoch=pending_effect_epoch,
                last_round_id=round_id,
            )
            return self._round_plan(
                next_state,
                round_id,
                mode=ActionMode.ADVANCE,
                effect_stage=effect,
                liveness_status=liveness_status,
            )
        if (
            state.pending_effect_stage is not EffectStage.NONE
            and self._effect_receipt_matches(snapshot, state)
        ):
            state = replace(
                state,
                pending_effect_stage=EffectStage.NONE,
                pending_effect_epoch=None,
            )
        if now < obligation.next_retry_at:
            return self._hold(
                state,
                round_id,
                liveness_status=(
                    "blocked"
                    if not snapshot.exchange_observation_available
                    else "recovering"
                ),
            )
        effect = (
            EffectStage.LOCAL_STATE_REPAIR
            if obligation.needs_manifest_rebuild
            else EffectStage.MANAGED_GTX_CANCEL
        )
        next_obligation = replace(
            obligation,
            attempt_count=obligation.attempt_count + 1,
            next_retry_at=now + self.policy.cleanup_retry_interval,
        )
        if (
            state.pending_effect_stage is effect
            and state.pending_effect_epoch is not None
        ):
            effect_epoch = state.effect_epoch
            pending_effect_epoch = state.pending_effect_epoch
        else:
            effect_epoch = state.effect_epoch + 1
            pending_effect_epoch = effect_epoch
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            cleanup_obligation=next_obligation,
            effect_epoch=effect_epoch,
            pending_effect_stage=effect,
            pending_effect_epoch=pending_effect_epoch,
            last_round_id=round_id,
        )
        return self._round_plan(
            next_state,
            round_id,
            mode=ActionMode.ADVANCE,
            effect_stage=effect,
            liveness_status=(
                "blocked"
                if not snapshot.exchange_observation_available
                else "recovering"
            ),
        )

    @staticmethod
    def _cleanup_proof_valid(
        proof: CleanupProof | None,
        obligation: CleanupObligation,
        *,
        captured_at: datetime,
    ) -> bool:
        return bool(
            proof is not None
            and proof.source_decision_id == obligation.source_decision_id
            and proof.action_lease_epoch == obligation.action_lease_epoch
            and not proof.remaining_order_ids
            and proof.user_trades_watermark >= 0
            and proof.observed_at >= obligation.created_at
            and proof.observed_at <= captured_at
        )

    def _plan_restoring(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        confirmed = (
            snapshot.applied_generation == state.generation
            and snapshot.applied_profile_digest == state.desired_profile.digest
        )
        if confirmed:
            if self._episode_blockers_cleared(snapshot.assessment):
                state = self._clear_episode(state)
            next_state = replace(
                state,
                document_revision=state.document_revision + 1,
                phase=RecoveryPhase.COOLDOWN,
                cooldown_until=now + self.policy.cooldown,
                progress_deadline_at=None,
                pending_effect_stage=EffectStage.NONE,
                pending_effect_epoch=None,
                last_round_id=round_id,
            )
            return self._round_plan(
                next_state,
                round_id,
                mode=ActionMode.ADVANCE,
                effect_stage=EffectStage.NONE,
                liveness_status="recovering",
            )
        if state.progress_deadline_at is None or now < state.progress_deadline_at:
            return self._hold(state, round_id, liveness_status="recovering")

        synthetic_assessment = replace(
            snapshot.assessment,
            runner_faults=tuple(
                dict.fromkeys(
                    (
                        *snapshot.assessment.runner_faults,
                        "restore_confirmation_timeout",
                    )
                )
            ),
        )
        synthetic_snapshot = replace(
            snapshot,
            assessment=synthetic_assessment,
        )
        candidates = self._candidates(synthetic_assessment, state)
        selected = next(
            candidate
            for candidate in candidates
            if candidate.action_id is ActionId.RUNNER_RECOVER
        )
        suppressed = (
            (state.active_action,)
            if state.active_action
            not in {
                ActionId.NOOP,
                ActionId.RUNNER_RECOVER,
            }
            else ()
        )
        return self._enter_candidate(
            snapshot=synthetic_snapshot,
            state=state,
            selected=selected,
            suppressed=suppressed,
            now=now,
            round_id=round_id,
        )

    def _plan_cooldown(
        self,
        snapshot: SymbolSnapshot,
        state: RecoveryState,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        if self._episode_blockers_cleared(snapshot.assessment):
            state = self._clear_episode(state)
        if state.cooldown_until is None or now < state.cooldown_until:
            return self._hold(state, round_id, liveness_status="recovering")
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            phase=RecoveryPhase.STABLE,
            active_action=ActionId.NOOP,
            decision_id=None,
            side=None,
            order_role=None,
            reasons=(),
            issued_at=None,
            progress_deadline_at=None,
            hard_expires_at=None,
            cooldown_until=None,
            action_lease=None,
            safety_lease=None,
            cleanup_obligation=None,
            cleanup_successor_action=None,
            post_cleanup_effect_stage=EffectStage.NONE,
            terminal_stop_confirmed=False,
            pending_effect_stage=EffectStage.NONE,
            pending_effect_epoch=None,
            last_round_id=round_id,
        )
        return RoundPlan(
            symbol=state.symbol,
            round_id=round_id,
            action_id=state.active_action,
            mode=ActionMode.EXIT,
            side=state.side,
            order_role=state.order_role,
            reasons=state.reasons,
            suppressed_actions=(),
            desired_profile=next_state.desired_profile,
            desired_runner_state=next_state.desired_runner_state,
            effect_stage=EffectStage.NONE,
            effect_epoch=None,
            liveness_status="healthy",
            next_state=next_state,
        )

    @staticmethod
    def _episode_blockers_cleared(
        assessment: FlowBlockerAssessment,
    ) -> bool:
        return not (
            assessment.inventory_reduce_sides
            or assessment.loss_only_blocked_sides
            or assessment.missing_entry_sides
        )

    @staticmethod
    def _clear_episode(state: RecoveryState) -> RecoveryState:
        return replace(
            state,
            exhausted_attempts=(),
            temporary_loss_lease_uses=MappingProxyType({}),
            active_episode_fingerprint=None,
        )

    def _hold(
        self,
        state: RecoveryState,
        round_id: str,
        *,
        liveness_status: str,
        effect_stage: EffectStage = EffectStage.NONE,
    ) -> RoundPlan:
        next_state = replace(
            state,
            document_revision=state.document_revision + 1,
            last_round_id=round_id,
        )
        return self._round_plan(
            next_state,
            round_id,
            mode=ActionMode.HOLD,
            effect_stage=effect_stage,
            liveness_status=liveness_status,
        )

    def _round_plan(
        self,
        state: RecoveryState,
        round_id: str,
        *,
        mode: ActionMode,
        effect_stage: EffectStage,
        liveness_status: str,
    ) -> RoundPlan:
        return RoundPlan(
            symbol=state.symbol,
            round_id=round_id,
            action_id=state.active_action,
            mode=mode,
            side=state.side,
            order_role=state.order_role,
            reasons=state.reasons,
            suppressed_actions=(),
            desired_profile=state.desired_profile,
            desired_runner_state=state.desired_runner_state,
            effect_stage=effect_stage,
            effect_epoch=(
                state.pending_effect_epoch
                if effect_stage is not EffectStage.NONE
                else None
            ),
            liveness_status=liveness_status,
            next_state=state,
        )

    @staticmethod
    def _has_current_progress(
        snapshot: SymbolSnapshot,
        state: RecoveryState,
    ) -> bool:
        return any(
            receipt.decision_id == state.decision_id
            and receipt.generation == state.generation
            and receipt.side is state.side
            and receipt.order_role is state.order_role
            and receipt.observation_seq >= 0
            and state.issued_at is not None
            and receipt.observed_at >= state.issued_at
            and receipt.observed_at <= snapshot.captured_at
            for receipt in snapshot.progress_receipts
        )

    def _active_still_eligible(
        self,
        assessment: FlowBlockerAssessment,
        state: RecoveryState,
    ) -> bool:
        return any(
            candidate.action_id is state.active_action
            and candidate.side is state.side
            and candidate.order_role is state.order_role
            for candidate in self._candidates(assessment, state)
        )

    def _candidates(
        self,
        assessment: FlowBlockerAssessment,
        state: RecoveryState,
    ) -> tuple[_Candidate, ...]:
        candidates: list[_Candidate] = []
        exhausted_candidates: list[_Candidate] = []
        noop_reasons: list[str] = []
        for definition in ACTION_DEFINITIONS:
            if definition.action_id is ActionId.NOOP:
                continue
            evaluation = definition.evaluate(assessment, state, self.policy)
            noop_reasons.extend(evaluation.blocked_reasons)
            for candidate in evaluation.candidates:
                if (
                    candidate.side is not None
                    and candidate.order_role is not None
                    and state.attempt_exhausted(
                        candidate.action_id,
                        candidate.side,
                        candidate.order_role,
                    )
                ):
                    noop_reasons.append(
                        "action_attempt_exhausted:"
                        f"{candidate.action_id.value}:"
                        f"{candidate.side.value}:"
                        f"{candidate.order_role.value}"
                    )
                    exhausted_candidates.append(candidate)
                    continue
                candidates.append(candidate)
        if not candidates and exhausted_candidates:
            candidates.extend(exhausted_candidates)
        candidates.append(
            _Candidate(ActionId.NOOP, reasons=tuple(dict.fromkeys(noop_reasons)))
        )
        return tuple(candidates)

    @staticmethod
    def _select_candidate(
        candidates: tuple[_Candidate, ...],
        state: RecoveryState,
    ) -> _Candidate:
        best_rank = min(_ACTION_RANK[item.action_id] for item in candidates)
        peers = tuple(
            item for item in candidates if _ACTION_RANK[item.action_id] == best_rank
        )
        if len(peers) == 1:
            return peers[0]

        directional = tuple(
            item
            for item in peers
            if item.side is not None and item.order_role is not None
        )
        if not directional:
            return peers[0]

        role = min(
            (item.order_role for item in directional if item.order_role is not None),
            key=lambda item: item.value,
        )
        role_peers = tuple(
            sorted(
                (item for item in directional if item.order_role is role),
                key=lambda item: item.side.value,
            )
        )
        fairness_key = _directional_fairness_key(
            role_peers[0].action_id,
            role,
        )
        last_side = state.fairness_last_selected_sides.get(fairness_key)
        if last_side is None:
            return role_peers[0]
        for index, candidate in enumerate(role_peers):
            if candidate.side is last_side:
                return role_peers[(index + 1) % len(role_peers)]
        return role_peers[0]

    @staticmethod
    def _fairness_after_selection(
        state: RecoveryState,
        selected: _Candidate,
    ) -> Mapping[str, Side]:
        fairness = dict(state.fairness_last_selected_sides)
        if selected.side is not None and selected.order_role is not None:
            fairness[
                _directional_fairness_key(
                    selected.action_id,
                    selected.order_role,
                )
            ] = selected.side
        return MappingProxyType(fairness)

    @staticmethod
    def _exhausted_after_selection(
        state: RecoveryState,
        selected: _Candidate,
    ) -> tuple[ActionAttempt, ...]:
        if selected.side is None or selected.order_role is None:
            return state.exhausted_attempts
        return tuple(
            attempt
            for attempt in state.exhausted_attempts
            if not (
                attempt.action_id is selected.action_id
                and attempt.order_role is selected.order_role
            )
        )


class RecoverySnapshotProvider(Protocol):
    def __call__(
        self,
        symbol: str,
        now: datetime,
        state: RecoveryState,
    ) -> SymbolSnapshot: ...


class RecoveryStore(Protocol):
    def read(self, symbol: str) -> RecoveryState: ...

    def compare_and_swap(
        self,
        symbol: str,
        *,
        expected_revision: int,
        next_state: RecoveryState,
    ) -> None: ...


class InMemoryRecoveryStore:
    """Small CAS store used by tests and shadow-mode simulations."""

    def __init__(self) -> None:
        self._states: dict[str, RecoveryState] = {}
        self._commit_counts: dict[str, int] = {}

    def register_symbol(
        self,
        symbol: str,
        baseline: Mapping[str, JsonValue],
        *,
        now: datetime,
    ) -> RecoveryState:
        normalized = symbol.upper().strip()
        if normalized in self._states:
            raise ValueError(f"symbol already registered: {normalized}")
        state = RecoveryState.initial(normalized, baseline, now=now)
        self._states[normalized] = state
        self._commit_counts[normalized] = 0
        return state

    def read(self, symbol: str) -> RecoveryState:
        normalized = symbol.upper().strip()
        try:
            return self._states[normalized]
        except KeyError as exc:
            raise KeyError(f"unregistered recovery symbol: {normalized}") from exc

    def compare_and_swap(
        self,
        symbol: str,
        *,
        expected_revision: int,
        next_state: RecoveryState,
    ) -> None:
        normalized = symbol.upper().strip()
        current = self.read(normalized)
        if current.document_revision != expected_revision:
            raise RuntimeError(
                f"recovery revision conflict for {normalized}: "
                f"expected {expected_revision}, actual {current.document_revision}"
            )
        if next_state.document_revision != expected_revision + 1:
            raise ValueError("one round must increment document_revision exactly once")
        self._states[normalized] = next_state
        self._commit_counts[normalized] = self._commit_counts.get(normalized, 0) + 1

    def commit_count(self, symbol: str) -> int:
        return self._commit_counts.get(symbol.upper().strip(), 0)


EffectExecutor = Callable[[str, EffectCommand], object]


class FuturesRecoveryCoordinator:
    """Orchestrate one immutable snapshot, one plan, and one CAS per symbol."""

    def __init__(
        self,
        *,
        store: RecoveryStore,
        snapshot_provider: RecoverySnapshotProvider,
        effect_executor: EffectExecutor | None = None,
        engine: FuturesRecoveryDecisionEngine | None = None,
    ) -> None:
        self.store = store
        self.snapshot_provider = snapshot_provider
        self.effect_executor = effect_executor or (lambda _symbol, _command: None)
        self.engine = engine or FuturesRecoveryDecisionEngine()
        self._round_outcomes: dict[tuple[str, str], RoundPlan] = {}

    def reconcile_symbol(
        self,
        symbol: str,
        *,
        now: datetime,
        round_id: str,
    ) -> RoundPlan:
        normalized = symbol.upper().strip()
        cache_key = (normalized, round_id)
        if cache_key in self._round_outcomes:
            return self._round_outcomes[cache_key]
        state = self.store.read(normalized)
        if round_id in state.recent_round_ids or state.last_round_id == round_id:
            replay = RoundPlan(
                symbol=state.symbol,
                round_id=round_id,
                action_id=state.active_action,
                mode=ActionMode.HOLD,
                side=state.side,
                order_role=state.order_role,
                reasons=state.reasons,
                suppressed_actions=(),
                desired_profile=state.desired_profile,
                desired_runner_state=state.desired_runner_state,
                effect_stage=EffectStage.NONE,
                effect_epoch=None,
                liveness_status=(
                    "terminal"
                    if state.phase
                    in {
                        RecoveryPhase.STOP_PENDING,
                        RecoveryPhase.STOPPED,
                    }
                    else "recovering"
                ),
                next_state=state,
            )
            self._round_outcomes[cache_key] = replay
            return replay
        snapshot = self.snapshot_provider(normalized, now, state)
        plan = self.engine.plan_round(
            snapshot=snapshot,
            state=state,
            now=now,
            round_id=round_id,
        )
        self.store.compare_and_swap(
            normalized,
            expected_revision=state.document_revision,
            next_state=plan.next_state,
        )
        if plan.effect_stage is not EffectStage.NONE:
            decision_id = plan.next_state.decision_id or f"{normalized}:{round_id}"
            if plan.effect_epoch is None:
                raise ValueError("effect command is missing its epoch")
            command = EffectCommand(
                decision_id=decision_id,
                stage=plan.effect_stage,
                effect_epoch=plan.effect_epoch,
            )
            try:
                self.effect_executor(normalized, command)
            except Exception as exc:
                plan = replace(plan, effect_error=str(exc))
        self._round_outcomes[cache_key] = plan
        return plan
