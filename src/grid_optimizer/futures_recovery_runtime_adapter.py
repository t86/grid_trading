"""Strict adapters for runner-issued futures recovery evidence.

Runner journals are evidence, not authority.  Each versioned loader verifies
the receipt hash and monotonic observation clock, then fences current evidence
to the exact coordinator decision, generation, profile and action window.  The
module performs no control mutation, restart, cancellation or exchange request.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any, Iterable, Mapping

from .futures_recovery_coordinator import (
    ActionId,
    ActivationReceipt,
    CleanupProof,
    EffectReceipt,
    EffectStage,
    LedgerClass,
    ManagedOrderIdentity,
    ManagedOrderManifest,
    OrderRole,
    ProgressReceipt,
    RoundPlan,
    FuturesRecoveryCoordinator,
    FuturesRecoveryDecisionEngine,
    RecoveryPhase,
    RecoveryState,
    Side,
    SymbolSnapshot,
)
from .futures_managed_order_manifest import ordinary_recovery_client_order_prefix
from .futures_trade_window import fetch_exact_futures_trade_window


TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY = "_futures_recovery_runner_receipts"
TEMPORARY_LOSS_RUNNER_RECEIPTS_SCHEMA = "temporary_loss_runner_receipts_v1"
ORDINARY_RECOVERY_RUNNER_RECEIPTS_KEY = (
    "_futures_ordinary_recovery_runner_receipts"
)
ORDINARY_RECOVERY_RUNNER_RECEIPTS_SCHEMA = (
    "ordinary_recovery_runner_receipts_v1"
)
# Compatibility names for the first ordinary action that used this journal.
INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY = ORDINARY_RECOVERY_RUNNER_RECEIPTS_KEY
INVENTORY_RECOVERY_RUNNER_RECEIPTS_SCHEMA = ORDINARY_RECOVERY_RUNNER_RECEIPTS_SCHEMA
RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY = (
    "_futures_recovery_config_applied_receipts"
)
RECOVERY_CONFIG_APPLIED_RECEIPTS_SCHEMA = (
    "futures_recovery_config_applied_receipts_v1"
)
_RECEIPT_ID_RE = re.compile(r"^tlr-[0-9a-f]{32}$")
_KINDS = frozenset(
    {
        "activation",
        "progress",
        "expiry",
        "cleanup_needed",
        "restoration_applied",
    }
)
_COMMON_FIELDS = frozenset(
    {
        "receipt_id",
        "kind",
        "symbol",
        "generation",
        "decision_id",
        "profile_digest",
        "action_lease_epoch",
        "side",
        "role",
        "order_role",
        "hard_expires_at",
        "observed_at",
        "observation_seq",
    }
)
_RESTORATION_COMMON_FIELDS = frozenset(
    {
        "receipt_id",
        "kind",
        "symbol",
        "generation",
        "decision_id",
        "profile_digest",
        "observed_at",
        "observation_seq",
    }
)
_DETAIL_FIELDS = {
    "activation": frozenset({"allow_loss_effective", "config_applied"}),
    "progress": frozenset(
        {
            "accepted_order_ids",
            "accepted_client_order_ids",
            "accepted_statuses",
            "allow_loss_effective",
            "progress_stage",
        }
    ),
    "expiry": frozenset({"allow_loss_effective", "expired_at", "reason"}),
    "cleanup_needed": frozenset(
        {
            "accepted_order_ids",
            "accepted_client_order_ids",
            "exchange_observation_required",
            "allow_loss_effective",
            "reason",
        }
    ),
    "restoration_applied": frozenset(
        {"allow_loss_effective", "config_applied"}
    ),
}


class TemporaryLossReceiptJournalError(RuntimeError):
    """The runner journal cannot be trusted as coordinator evidence."""


class OrdinaryRecoveryReceiptJournalError(RuntimeError):
    """Ordinary recovery progress evidence is malformed or unfenced."""


InventoryRecoveryReceiptJournalError = OrdinaryRecoveryReceiptJournalError


class RecoveryConfigAppliedReceiptJournalError(RuntimeError):
    """Runner config-applied evidence is malformed or unfenced."""


class RecoveryAdmissionEvidenceError(RuntimeError):
    """Latest plan/submit reports cannot authorize an ordinary recovery action."""


@dataclass(frozen=True)
class TemporaryLossRuntimeEvidence:
    activation_receipt: ActivationReceipt | None = None
    progress_receipts: tuple[ProgressReceipt, ...] = ()
    accepted_order_ids: tuple[str, ...] = ()
    accepted_client_order_ids: tuple[str, ...] = ()
    accepted_order_pairs: tuple[tuple[str, str], ...] = ()
    cleanup_requested: bool = False
    cleanup_order_ids: tuple[str, ...] = ()
    cleanup_client_order_ids: tuple[str, ...] = ()
    latest_observation_seq: int = -1
    applied_generation: int | None = None
    applied_profile_digest: str | None = None


@dataclass(frozen=True)
class OrdinaryRecoveryRuntimeEvidence:
    progress_receipts: tuple[ProgressReceipt, ...] = ()
    accepted_order_ids: tuple[str, ...] = ()
    accepted_client_order_ids: tuple[str, ...] = ()
    accepted_order_pairs: tuple[tuple[str, str], ...] = ()
    latest_observation_seq: int = -1


InventoryRecoveryRuntimeEvidence = OrdinaryRecoveryRuntimeEvidence


@dataclass(frozen=True)
class RecoveryConfigAppliedEvidence:
    applied_generation: int | None = None
    applied_profile_digest: str | None = None
    receipt_id: str | None = None
    runner_instance_id: str | None = None
    observed_at: datetime | None = None
    latest_observation_seq: int = -1


@dataclass(frozen=True)
class RecoveryAdmissionEvidence:
    plan_observed_at: datetime
    submit_observed_at: datetime
    loss_only_blocked_sides: tuple[Side, ...] = ()


@dataclass(frozen=True)
class ManagedGtxCleanupResult:
    canceled_order_ids: tuple[str, ...] = ()
    already_terminal_order_ids: tuple[str, ...] = ()
    remaining_order_ids: tuple[str, ...] = ()
    effect_receipt: EffectReceipt | None = None
    cleanup_proof: CleanupProof | None = None
    blocked_reason: str | None = None


@dataclass(frozen=True)
class RegisteredRecoveryRoundResult:
    plan: RoundPlan
    snapshot_provider_calls: int
    control_cas_count: int
    effect_count: int


_RECOVERY_GATE_FIELDS = frozenset(
    {
        "managed",
        "ready",
        "reason",
        "symbol",
        "generation",
        "decision_id",
        "profile_digest",
        "active_action",
        "side",
        "order_role",
        "ledger_class",
        "allowed_orders",
        "allowed_roles",
        "progress_deadline_at",
        "hard_expires_at",
    }
)
_PLAN_RECOVERY_GATE_FIELDS = _RECOVERY_GATE_FIELDS | frozenset(
    {"dropped_order_count", "dropped_orders"}
)
_RECOVERY_EXECUTION_FIELDS = frozenset(
    {
        "managed",
        "authorized",
        "reason",
        "current_gate",
        "dropped_place_count",
        "dropped_cancel_count",
        "dropped_orders",
    }
)


def _admission_fail(message: str) -> RecoveryAdmissionEvidenceError:
    return RecoveryAdmissionEvidenceError(message)


def _admission_datetime(value: Any, *, path: str) -> datetime:
    if not isinstance(value, str):
        raise _admission_fail(f"{path} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _admission_fail(f"{path} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _admission_fail(f"{path} must be timezone-aware")
    return parsed


def _normal_recovery_profile_gate(state: RecoveryState) -> bool:
    return bool(
        state.phase in {RecoveryPhase.STABLE, RecoveryPhase.COOLDOWN}
        or (
            state.phase is RecoveryPhase.RESTORING
            and state.action_lease is None
            and state.cleanup_obligation is None
            and state.safety_lease is None
            and state.desired_runner_state == "running"
            and state.desired_profile.digest == state.baseline_profile.digest
        )
    )


def _expected_recovery_profile_gate(state: RecoveryState) -> dict[str, Any]:
    normal = _normal_recovery_profile_gate(state)
    action = ActionId.NOOP if normal else state.active_action
    side = None if normal else state.side
    order_role = None if normal else state.order_role
    progress_deadline = None if normal else state.progress_deadline_at
    hard_expires = None if normal else state.hard_expires_at
    ledger_class: str | None = None
    allowed_orders: list[dict[str, str]] = []
    if action is ActionId.INVENTORY_RECOVER and side is not None:
        ledger_class = LedgerClass.NORMAL_BQ.value
        allowed_orders = [
            {
                "role": (
                    "best_quote_reduce_long"
                    if side is Side.SELL
                    else "best_quote_reduce_short"
                ),
                "side": side.value,
            }
        ]
    elif action is ActionId.MAKER_FLOW_RECOVER and side is not None:
        ledger_class = LedgerClass.NORMAL_BQ.value
        allowed_orders = [
            {
                "role": (
                    "best_quote_entry_long"
                    if side is Side.BUY
                    else "best_quote_entry_short"
                ),
                "side": side.value,
            }
        ]
    elif (
        action is ActionId.BASELINE_TUNE
        and side is not None
        and order_role in {OrderRole.ENTRY, OrderRole.REDUCE_ONLY}
    ):
        ledger_class = LedgerClass.NORMAL_BQ.value
        roles = {
            (OrderRole.ENTRY, Side.BUY): "best_quote_entry_long",
            (OrderRole.ENTRY, Side.SELL): "best_quote_entry_short",
            (OrderRole.REDUCE_ONLY, Side.SELL): "best_quote_reduce_long",
            (OrderRole.REDUCE_ONLY, Side.BUY): "best_quote_reduce_short",
        }
        allowed_orders = [
            {
                "role": roles[(order_role, side)],
                "side": side.value,
            }
        ]
    elif action is ActionId.TEMPORARY_LOSS_RELIEF and side is not None:
        ledger_class = LedgerClass.NORMAL_BQ.value
        allowed_orders = [
            {
                "role": (
                    "best_quote_reduce_long"
                    if side is Side.SELL
                    else "best_quote_reduce_short"
                ),
                "side": side.value,
            }
        ]
    elif action is ActionId.SAFETY_CONVERGE:
        ledger_class = LedgerClass.NORMAL_BQ.value
        reduce_sides = state.desired_profile.fields.get(
            "recovery_reduce_only_allowed_sides"
        )
        if isinstance(reduce_sides, tuple):
            allowed_orders = [
                {
                    "role": (
                        "best_quote_reduce_long"
                        if value == Side.SELL.value
                        else "best_quote_reduce_short"
                    ),
                    "side": value,
                }
                for value in sorted(set(reduce_sides))
                if value in {Side.BUY.value, Side.SELL.value}
            ]
    return {
        "managed": True,
        "ready": True,
        "reason": None,
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
        "active_action": action.value,
        "side": side.value if side is not None else None,
        "order_role": order_role.value if order_role is not None else None,
        "ledger_class": ledger_class,
        "allowed_orders": allowed_orders,
        "allowed_roles": [item["role"] for item in allowed_orders],
        "progress_deadline_at": (
            progress_deadline.isoformat()
            if progress_deadline is not None
            else None
        ),
        "hard_expires_at": (
            hard_expires.isoformat() if hard_expires is not None else None
        ),
    }


def load_recovery_admission_evidence(
    *,
    plan: Mapping[str, Any],
    submit: Mapping[str, Any],
    recovery_state: RecoveryState,
    captured_at: datetime,
    max_age_seconds: float,
) -> RecoveryAdmissionEvidence:
    """Validate current plan+submit fences before mapping ordinary blockers.

    These reports have no durable observation sequence and therefore prove
    admission/readiness only.  They must never be converted into a progress
    receipt.
    """

    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    if max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be positive")
    plan_at = _admission_datetime(plan.get("generated_at"), path="plan.generated_at")
    submit_at = _admission_datetime(
        submit.get("submit_generated_at"),
        path="submit.submit_generated_at",
    )
    if plan_at > submit_at:
        raise _admission_fail("submit report predates its plan")
    if submit_at > captured_at:
        raise _admission_fail("submit report is after its snapshot")
    if recovery_state.issued_at is not None and plan_at < recovery_state.issued_at:
        raise _admission_fail("plan report predates the current recovery decision")
    if (
        recovery_state.progress_deadline_at is not None
        and captured_at >= recovery_state.progress_deadline_at
    ):
        raise _admission_fail("recovery admission window has reached its deadline")
    max_fresh_age = min(float(max_age_seconds), 30.0)
    if (captured_at - plan_at).total_seconds() > max_fresh_age:
        raise _admission_fail("plan is stale for recovery admission")

    raw_plan_gate = plan.get("recovery_profile_gate")
    if not isinstance(raw_plan_gate, Mapping) or set(raw_plan_gate) != set(
        _PLAN_RECOVERY_GATE_FIELDS
    ):
        raise _admission_fail("plan recovery gate shape is invalid")
    if (
        type(raw_plan_gate.get("dropped_order_count")) is not int
        or int(raw_plan_gate["dropped_order_count"]) < 0
        or not isinstance(raw_plan_gate.get("dropped_orders"), list)
    ):
        raise _admission_fail("plan recovery gate drop report is invalid")
    plan_gate = {
        key: raw_plan_gate[key] for key in _RECOVERY_GATE_FIELDS
    }
    expected_gate = _expected_recovery_profile_gate(recovery_state)
    if plan_gate != expected_gate:
        raise _admission_fail("plan recovery gate does not match current state")

    raw_execution = submit.get("recovery_profile_execution")
    if not isinstance(raw_execution, Mapping) or set(raw_execution) != set(
        _RECOVERY_EXECUTION_FIELDS
    ):
        raise _admission_fail("submit recovery execution shape is invalid")
    current_gate = raw_execution.get("current_gate")
    if (
        not isinstance(current_gate, Mapping)
        or set(current_gate) != set(_RECOVERY_GATE_FIELDS)
        or dict(current_gate) != plan_gate
    ):
        raise _admission_fail("submit recovery gate does not match plan")
    if not (
        raw_execution.get("managed") is True
        and raw_execution.get("authorized") is True
        and raw_execution.get("reason") is None
        and type(raw_execution.get("dropped_place_count")) is int
        and int(raw_execution["dropped_place_count"]) >= 0
        and type(raw_execution.get("dropped_cancel_count")) is int
        and int(raw_execution["dropped_cancel_count"]) >= 0
        and isinstance(raw_execution.get("dropped_orders"), list)
    ):
        raise _admission_fail("submit recovery execution is not authorized")

    actions = (
        submit.get("validation", {}).get("actions", {})
        if isinstance(submit.get("validation"), Mapping)
        else {}
    )
    no_loss = (
        actions.get("reduce_only_no_loss_guard", {})
        if isinstance(actions, Mapping)
        else {}
    )
    dropped = (
        no_loss.get("dropped_orders", [])
        if isinstance(no_loss, Mapping)
        else []
    )
    loss_sides: set[Side] = set()
    for item in dropped if isinstance(dropped, list) else []:
        if not isinstance(item, Mapping):
            continue
        reason = item.get("reduce_only_no_loss_drop_reason")
        role = item.get("role")
        side = item.get("side")
        if (
            reason == "long_reduce_below_no_loss_floor"
            and role == "best_quote_reduce_long"
            and side == Side.SELL.value
        ):
            loss_sides.add(Side.SELL)
        elif (
            reason == "short_reduce_above_no_loss_ceiling"
            and role == "best_quote_reduce_short"
            and side == Side.BUY.value
        ):
            loss_sides.add(Side.BUY)
    return RecoveryAdmissionEvidence(
        plan_observed_at=plan_at,
        submit_observed_at=submit_at,
        loss_only_blocked_sides=tuple(
            sorted(loss_sides, key=lambda value: value.value)
        ),
    )


class _OneRoundStore:
    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.control_cas_count = 0

    def read(self, symbol: str) -> RecoveryState:
        return self.delegate.read(symbol)

    def compare_and_swap(
        self,
        symbol: str,
        *,
        expected_revision: int,
        next_state: RecoveryState,
    ) -> None:
        self.control_cas_count += 1
        if self.control_cas_count > 1:
            raise RuntimeError("one registered symbol round attempted multiple control CAS writes")
        self.delegate.compare_and_swap(
            symbol,
            expected_revision=expected_revision,
            next_state=next_state,
        )


class RegisteredSymbolRecoveryOrchestrator:
    """Execute exactly one immutable coordinator round for one symbol."""

    def __init__(
        self,
        *,
        store: Any,
        effect_executor: Any,
        engine: FuturesRecoveryDecisionEngine | None = None,
    ) -> None:
        self.store = store
        self.effect_executor = effect_executor
        self.engine = engine

    def reconcile(
        self,
        *,
        snapshot: SymbolSnapshot,
        now: datetime,
        round_id: str,
    ) -> RegisteredRecoveryRoundResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if snapshot.captured_at != now:
            raise ValueError("registered recovery snapshot must be captured once at round time")
        symbol = snapshot.symbol.upper().strip()
        if not symbol or snapshot.symbol != symbol:
            raise ValueError("registered recovery snapshot symbol must be canonical")
        snapshot_calls = 0
        effect_count = 0
        guarded_store = _OneRoundStore(self.store)

        def provide_snapshot(
            requested_symbol: str,
            requested_now: datetime,
            _state: RecoveryState,
        ) -> SymbolSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            if snapshot_calls > 1:
                raise RuntimeError("one registered symbol round requested multiple snapshots")
            if requested_symbol != symbol or requested_now != now:
                raise RuntimeError("coordinator changed the immutable round identity")
            return snapshot

        def execute_effect(requested_symbol: str, command: Any) -> Any:
            nonlocal effect_count
            effect_count += 1
            if effect_count > 1:
                raise RuntimeError("one registered symbol round attempted multiple effects")
            return self.effect_executor(requested_symbol, command)

        coordinator = FuturesRecoveryCoordinator(
            store=guarded_store,
            snapshot_provider=provide_snapshot,
            effect_executor=execute_effect,
            engine=self.engine,
        )
        plan = coordinator.reconcile_symbol(symbol, now=now, round_id=round_id)
        if snapshot_calls != 1 or guarded_store.control_cas_count != 1:
            raise RuntimeError("registered recovery round did not consume one snapshot and one CAS")
        return RegisteredRecoveryRoundResult(
            plan=plan,
            snapshot_provider_calls=snapshot_calls,
            control_cas_count=guarded_store.control_cas_count,
            effect_count=effect_count,
        )


def _fail(message: str) -> TemporaryLossReceiptJournalError:
    return TemporaryLossReceiptJournalError(message)


def temporary_loss_client_order_prefix_for_binding(
    *,
    symbol: str,
    source_decision_id: str,
    action_lease_epoch: int,
    side: Side,
    hard_expires_at: datetime,
) -> str:
    """Return one compact prefix that can reconstruct a lease after a crash."""

    normalized_symbol = str(symbol).upper().strip()
    decision_id = str(source_decision_id).strip()
    if not (
        normalized_symbol
        and decision_id
        and type(action_lease_epoch) is int
        and action_lease_epoch >= 0
        and isinstance(side, Side)
        and hard_expires_at.tzinfo is not None
        and hard_expires_at.utcoffset() is not None
    ):
        raise _fail("temporary-loss client id binding is invalid")
    binding = {
        "symbol": normalized_symbol,
        "source_decision_id": decision_id,
        "action_lease_epoch": action_lease_epoch,
        "side": side.value,
        "hard_expires_at": hard_expires_at.isoformat(),
    }
    digest = hashlib.sha256(
        json.dumps(
            binding,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:12]
    compact_symbol = normalized_symbol.lower().replace("usdt", "u")
    prefix = f"gx-{compact_symbol}-tlr-{digest}-"
    # Binance caps newClientOrderId at 36 characters.  Keep six characters for
    # a per-submission nonce; an unusually long symbol fails closed instead of
    # weakening the lease digest or emitting an unreconstructable id.
    if len(prefix) > 30:
        raise _fail("symbol is too long for a reconstructable loss-lease client id")
    return prefix


def temporary_loss_client_order_prefix(
    recovery_state: RecoveryState,
) -> str:
    """Return the reconstructable client-id prefix for one typed loss lease."""

    lease = recovery_state.action_lease
    obligation = recovery_state.cleanup_obligation
    source_decision_id = (
        obligation.source_decision_id
        if obligation is not None
        and obligation.source_action_id is ActionId.TEMPORARY_LOSS_RELIEF
        else recovery_state.decision_id
    )
    if not (
        source_decision_id
        and lease is not None
        and lease.action_id is ActionId.TEMPORARY_LOSS_RELIEF
        and lease.order_role is OrderRole.REDUCE_ONLY
    ):
        raise _fail("temporary-loss client id requires one typed lease binding")
    return temporary_loss_client_order_prefix_for_binding(
        symbol=recovery_state.symbol,
        source_decision_id=str(source_decision_id),
        action_lease_epoch=lease.epoch,
        side=lease.side,
        hard_expires_at=lease.hard_expires_at,
    )


def _absolute_datetime(value: Any, *, path: str) -> datetime:
    if not isinstance(value, str):
        raise _fail(f"{path} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _fail(f"{path} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _fail(f"{path} must be timezone-aware")
    return parsed


def _string_list(
    value: Any,
    *,
    path: str,
    require_sorted: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise _fail(f"{path} must be a list")
    normalized: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip() or item != item.strip():
            raise _fail(f"{path}[{index}] must be a non-empty canonical string")
        normalized.append(item)
    if len(normalized) != len(set(normalized)) or (
        require_sorted and normalized != sorted(normalized)
    ):
        raise _fail(
            f"{path} must be unique"
            + (" and sorted" if require_sorted else "")
        )
    return tuple(normalized)


def _event_binding(event: Mapping[str, Any]) -> dict[str, Any]:
    if event.get("kind") == "restoration_applied":
        return {
            key: event[key]
            for key in ("symbol", "generation", "decision_id", "profile_digest")
        }
    return {
        key: event[key]
        for key in (
            "symbol",
            "generation",
            "decision_id",
            "profile_digest",
            "action_lease_epoch",
            "side",
            "role",
            "order_role",
            "hard_expires_at",
        )
    }


def _receipt_id(
    *,
    kind: str,
    binding: Mapping[str, Any],
    identity: Mapping[str, Any] | None = None,
) -> str:
    try:
        encoded = json.dumps(
            {
                "kind": kind,
                "binding": dict(binding),
                "identity": dict(identity or {}),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _fail("receipt binding is not canonical JSON") from exc
    return "tlr-" + hashlib.sha256(encoded).hexdigest()[:32]


def _validate_event_shape(
    event: Any,
    *,
    index: int,
) -> tuple[dict[str, Any], datetime, tuple[str, ...], tuple[str, ...]]:
    path = f"journal.events[{index}]"
    if not isinstance(event, Mapping):
        raise _fail(f"{path} must be an object")
    row = dict(event)
    kind = row.get("kind")
    if not isinstance(kind, str) or kind not in _KINDS:
        raise _fail(f"{path}.kind is unsupported")
    common_fields = (
        _RESTORATION_COMMON_FIELDS
        if kind == "restoration_applied"
        else _COMMON_FIELDS
    )
    expected_fields = common_fields | _DETAIL_FIELDS[kind]
    if set(row) != expected_fields:
        missing = sorted(expected_fields.difference(row))
        unexpected = sorted(set(row).difference(expected_fields))
        raise _fail(
            f"{path} fields are invalid; missing={missing}, unexpected={unexpected}"
        )
    if not isinstance(row["receipt_id"], str) or not _RECEIPT_ID_RE.fullmatch(
        row["receipt_id"]
    ):
        raise _fail(f"{path}.receipt_id is invalid")
    string_keys = ["symbol", "decision_id", "profile_digest"]
    if kind != "restoration_applied":
        string_keys.extend(("side", "role", "order_role"))
    for key in string_keys:
        if not isinstance(row[key], str) or not row[key]:
            raise _fail(f"{path}.{key} must be a non-empty string")
    if row["symbol"] != row["symbol"].upper().strip():
        raise _fail(f"{path}.symbol must be canonical")
    integer_keys = ["generation", "observation_seq"]
    if kind != "restoration_applied":
        integer_keys.append("action_lease_epoch")
    for key in integer_keys:
        if type(row[key]) is not int or row[key] < 0:
            raise _fail(f"{path}.{key} must be a non-negative integer")
    observed_at = _absolute_datetime(row["observed_at"], path=f"{path}.observed_at")
    if kind != "restoration_applied":
        _absolute_datetime(row["hard_expires_at"], path=f"{path}.hard_expires_at")
    order_ids: tuple[str, ...] = ()
    client_order_ids: tuple[str, ...] = ()
    identity: dict[str, Any] = {}
    if kind in {"progress", "cleanup_needed"}:
        order_ids = _string_list(
            row["accepted_order_ids"], path=f"{path}.accepted_order_ids"
        )
        client_order_ids = _string_list(
            row["accepted_client_order_ids"],
            path=f"{path}.accepted_client_order_ids",
            require_sorted=False,
        )
        if not order_ids and not client_order_ids and kind == "progress":
            raise _fail(f"{path} progress has no accepted order identity")
        if kind == "progress":
            if not order_ids or len(order_ids) != len(client_order_ids):
                raise _fail(
                    f"{path} progress must bind every exchange order id to a client id"
                )
            if tuple(zip(order_ids, client_order_ids, strict=True)) != tuple(
                sorted(set(zip(order_ids, client_order_ids, strict=True)))
            ):
                raise _fail(f"{path} progress order pairs are not canonical")
            identity = {
                "accepted_order_ids": list(order_ids),
                "accepted_client_order_ids": list(client_order_ids),
            }
            statuses = _string_list(
                row["accepted_statuses"], path=f"{path}.accepted_statuses"
            )
            if not statuses or not set(statuses).issubset(
                {"NEW", "PARTIALLY_FILLED", "FILLED"}
            ):
                raise _fail(f"{path}.accepted_statuses is invalid")
            if row["progress_stage"] != "gtx_order_accepted":
                raise _fail(f"{path}.progress_stage is invalid")
            if row["allow_loss_effective"] is not True:
                raise _fail(f"{path} progress did not observe active allow_loss")
        else:
            if row["exchange_observation_required"] is not True:
                raise _fail(f"{path} cleanup must require exchange observation")
            if row["allow_loss_effective"] is not False:
                raise _fail(f"{path} cleanup did not disable allow_loss")
            if row["reason"] != "temporary_loss_lease_orders_must_reconcile":
                raise _fail(f"{path}.reason is invalid")
    elif kind in {"activation", "restoration_applied"}:
        if row["config_applied"] is not True or row["allow_loss_effective"] is not False:
            raise _fail(f"{path} activation profile evidence is invalid")
    else:
        if row["allow_loss_effective"] is not False:
            raise _fail(f"{path} expiry did not disable allow_loss")
        if row["expired_at"] != row["hard_expires_at"]:
            raise _fail(f"{path}.expired_at does not match hard expiry")
        if row["reason"] != "temporary_loss_action_lease_hard_expired":
            raise _fail(f"{path}.reason is invalid")
    expected_id = _receipt_id(
        kind=kind,
        binding=_event_binding(row),
        identity=identity,
    )
    if row["receipt_id"] != expected_id:
        raise _fail(f"{path}.receipt_id does not authenticate its binding")
    return row, observed_at, order_ids, client_order_ids


def _current_binding(state: RecoveryState) -> dict[str, Any]:
    lease = state.action_lease
    if not (
        state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
        and state.phase in {RecoveryPhase.SETTLING, RecoveryPhase.ACTIVE}
        and state.decision_id
        and lease is not None
        and lease.action_id is ActionId.TEMPORARY_LOSS_RELIEF
        and lease.order_role is OrderRole.REDUCE_ONLY
        and state.side is lease.side
        and state.order_role is lease.order_role
        and state.action_lease_epoch == lease.epoch
        and state.hard_expires_at == lease.hard_expires_at
    ):
        raise _fail("recovery state is not a fenced temporary-loss lease")
    role = "best_quote_reduce_long" if lease.side is Side.SELL else "best_quote_reduce_short"
    return {
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
        "action_lease_epoch": lease.epoch,
        "side": lease.side.value,
        "role": role,
        "order_role": lease.order_role.value,
        "hard_expires_at": lease.hard_expires_at.isoformat(),
    }


def _restoration_binding(state: RecoveryState) -> dict[str, Any]:
    if not (
        state.phase is RecoveryPhase.RESTORING
        and state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
        and state.decision_id
        and state.action_lease is None
        and state.cleanup_obligation is None
    ):
        raise _fail("recovery state is not a temporary-loss restoration")
    return {
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
    }


def load_temporary_loss_runtime_evidence(
    *,
    state_path: str | Path,
    recovery_state: RecoveryState,
    captured_at: datetime,
) -> TemporaryLossRuntimeEvidence:
    """Parse and fence one runner journal for the current coordinator lease."""

    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    restoring = recovery_state.phase is RecoveryPhase.RESTORING
    binding = (
        _restoration_binding(recovery_state)
        if restoring
        else _current_binding(recovery_state)
    )
    try:
        raw_document = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return TemporaryLossRuntimeEvidence()
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _fail("runner state document is unreadable") from exc
    if not isinstance(raw_document, Mapping):
        raise _fail("runner state document must be an object")
    raw_journal = raw_document.get(TEMPORARY_LOSS_RUNNER_RECEIPTS_KEY)
    if raw_journal is None:
        return TemporaryLossRuntimeEvidence()
    if not isinstance(raw_journal, Mapping) or set(raw_journal) != {
        "schema",
        "symbol",
        "next_observation_seq",
        "events",
        "latest",
    }:
        raise _fail("runner receipt journal envelope is invalid")
    if raw_journal.get("schema") != TEMPORARY_LOSS_RUNNER_RECEIPTS_SCHEMA:
        raise _fail("runner receipt journal schema is unsupported")
    if raw_journal.get("symbol") != recovery_state.symbol:
        raise _fail("runner receipt journal symbol mismatch")
    next_seq = raw_journal.get("next_observation_seq")
    events = raw_journal.get("events")
    latest = raw_journal.get("latest")
    if type(next_seq) is not int or next_seq < 0:
        raise _fail("runner receipt next observation sequence is invalid")
    if not isinstance(events, list) or not isinstance(latest, Mapping):
        raise _fail("runner receipt journal collections are invalid")

    parsed: list[tuple[dict[str, Any], datetime, tuple[str, ...], tuple[str, ...]]] = []
    seen_ids: set[str] = set()
    previous_seq = -1
    expected_latest: dict[str, str] = {}
    for index, event in enumerate(events):
        item = _validate_event_shape(event, index=index)
        row = item[0]
        seq = int(row["observation_seq"])
        if seq <= previous_seq:
            raise _fail("runner receipt observation sequence is not strictly increasing")
        previous_seq = seq
        receipt_id = str(row["receipt_id"])
        if receipt_id in seen_ids:
            raise _fail("runner receipt id is duplicated")
        seen_ids.add(receipt_id)
        expected_latest[str(row["kind"])] = receipt_id
        parsed.append(item)
    if events and next_seq != previous_seq + 1:
        raise _fail("runner receipt next observation sequence is not contiguous")
    if not events and next_seq != 0:
        raise _fail("empty runner receipt journal has a non-zero sequence")
    if dict(latest) != expected_latest:
        raise _fail("runner receipt latest index does not match event history")

    activation: ActivationReceipt | None = None
    progress_order_ids: set[str] = set()
    progress_client_ids: set[str] = set()
    progress_order_pairs: set[tuple[str, str]] = set()
    cleanup_requested = False
    cleanup_order_ids: tuple[str, ...] = ()
    cleanup_client_ids: tuple[str, ...] = ()
    lease = recovery_state.action_lease
    if not restoring:
        assert lease is not None
    applied_generation: int | None = None
    applied_profile_digest: str | None = None
    for row, observed_at, order_ids, client_ids in parsed:
        kind = str(row["kind"])
        if kind == "restoration_applied":
            if not restoring or row["decision_id"] != binding["decision_id"]:
                continue
            for key, expected in binding.items():
                if row[key] != expected:
                    raise _fail(
                        f"runner restoration receipt fence mismatch: {key}"
                    )
            if observed_at > captured_at:
                raise _fail("runner restoration receipt is newer than its snapshot")
            if (
                recovery_state.issued_at is not None
                and observed_at < recovery_state.issued_at
            ):
                raise _fail("runner restoration receipt predates its decision")
            if applied_generation is not None:
                raise _fail("current restoration has multiple applied receipts")
            applied_generation = int(row["generation"])
            applied_profile_digest = str(row["profile_digest"])
            continue
        if restoring:
            continue
        same_decision = row["decision_id"] == binding["decision_id"]
        same_epoch = row["action_lease_epoch"] == binding["action_lease_epoch"]
        if same_decision != same_epoch:
            raise _fail("runner receipt partially matches the current lease identity")
        if not same_decision:
            continue
        # Activation belongs to the settling generation.  It remains in the
        # journal after activation advances generation and is historical then.
        relevant = bool(
            (recovery_state.phase is RecoveryPhase.SETTLING and kind == "activation")
            or (
                recovery_state.phase is RecoveryPhase.ACTIVE
                and kind in {"progress", "expiry", "cleanup_needed"}
            )
        )
        if not relevant:
            continue
        for key, expected in binding.items():
            if row[key] != expected:
                raise _fail(f"runner receipt current lease fence mismatch: {key}")
        if observed_at > captured_at:
            raise _fail("runner receipt is newer than its snapshot")
        if recovery_state.issued_at is not None and observed_at < recovery_state.issued_at:
            raise _fail("runner receipt predates its coordinator decision")
        if kind == "activation":
            if activation is not None:
                raise _fail("current lease has multiple activation receipts")
            if observed_at >= lease.hard_expires_at:
                raise _fail("activation receipt is at or after hard expiry")
            activation = ActivationReceipt(
                decision_id=str(row["decision_id"]),
                generation=int(row["generation"]),
                profile_digest=str(row["profile_digest"]),
                action_lease_epoch=int(row["action_lease_epoch"]),
                observed_at=observed_at,
            )
        elif kind == "progress":
            if lease.activated_at is None or observed_at < lease.activated_at:
                raise _fail("progress receipt predates lease activation")
            if observed_at >= lease.hard_expires_at:
                raise _fail("progress receipt is at or after hard expiry")
            progress_order_ids.update(order_ids)
            progress_client_ids.update(client_ids)
            progress_order_pairs.update(zip(order_ids, client_ids, strict=True))
        elif kind == "expiry":
            if observed_at < lease.hard_expires_at:
                raise _fail("expiry receipt predates hard expiry")
        else:
            if observed_at < lease.hard_expires_at:
                raise _fail("cleanup receipt predates hard expiry")
            if cleanup_requested:
                raise _fail("current lease has multiple cleanup receipts")
            cleanup_requested = True
            cleanup_order_ids = order_ids
            cleanup_client_ids = client_ids

    accepted_order_ids = tuple(sorted(progress_order_ids))
    accepted_client_ids = tuple(sorted(progress_client_ids))
    accepted_order_pairs = tuple(sorted(progress_order_pairs))
    if cleanup_requested:
        try:
            cleanup_pairs = tuple(
                sorted(zip(cleanup_order_ids, cleanup_client_ids, strict=True))
            )
        except ValueError as exc:
            raise _fail("cleanup manifest has incomplete order identity") from exc
        # Appending expiry/cleanup receipts trims the bounded journal.  The
        # cleanup event was built before that trim, so it may legitimately
        # retain identities whose progress events are no longer present.  It
        # must still contain every retained progress pair without remapping
        # either side of an identity.
        if not set(accepted_order_pairs).issubset(cleanup_pairs):
            raise _fail("cleanup manifest does not match progress manifest")
        accepted_order_pairs = cleanup_pairs
        accepted_order_ids = tuple(sorted(cleanup_order_ids))
        accepted_client_ids = tuple(sorted(cleanup_client_ids))
    if accepted_order_pairs:
        expected_client_prefix = temporary_loss_client_order_prefix(recovery_state)
        if any(
            not client_order_id.startswith(expected_client_prefix)
            for _order_id, client_order_id in accepted_order_pairs
        ):
            raise _fail(
                "temporary-loss manifest is outside its decision-bound client order prefix"
            )
    return TemporaryLossRuntimeEvidence(
        activation_receipt=activation,
        progress_receipts=(),
        accepted_order_ids=accepted_order_ids,
        accepted_client_order_ids=accepted_client_ids,
        accepted_order_pairs=accepted_order_pairs,
        cleanup_requested=cleanup_requested,
        cleanup_order_ids=cleanup_order_ids,
        cleanup_client_order_ids=cleanup_client_ids,
        latest_observation_seq=previous_seq,
        applied_generation=applied_generation,
        applied_profile_digest=applied_profile_digest,
    )


def _config_applied_fail(
    message: str,
) -> RecoveryConfigAppliedReceiptJournalError:
    return RecoveryConfigAppliedReceiptJournalError(message)


def _config_applied_datetime(value: Any, *, path: str) -> datetime:
    if not isinstance(value, str):
        raise _config_applied_fail(f"{path} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _config_applied_fail(f"{path} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _config_applied_fail(f"{path} must be timezone-aware")
    return parsed


def _config_applied_binding(state: RecoveryState) -> dict[str, Any] | None:
    if not state.decision_id or state.issued_at is None:
        return None
    return {
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": str(state.decision_id),
        "profile_digest": state.desired_profile.digest,
    }


def _config_applied_receipt_id(
    *,
    binding: Mapping[str, Any],
    runner_instance_id: str,
) -> str:
    encoded = json.dumps(
        {
            "kind": "config_applied",
            "binding": dict(binding),
            "runner_instance_id": runner_instance_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "rca-" + hashlib.sha256(encoded).hexdigest()[:32]


def load_recovery_config_applied_evidence(
    *,
    state_path: str | Path,
    recovery_state: RecoveryState,
    captured_at: datetime,
) -> RecoveryConfigAppliedEvidence:
    """Load a real runner-start proof for the exact current config generation."""

    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    binding = _config_applied_binding(recovery_state)
    if binding is None:
        return RecoveryConfigAppliedEvidence()
    try:
        document = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return RecoveryConfigAppliedEvidence()
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _config_applied_fail("runner state document is unreadable") from exc
    if not isinstance(document, Mapping):
        raise _config_applied_fail("runner state document must be an object")
    raw = document.get(RECOVERY_CONFIG_APPLIED_RECEIPTS_KEY)
    if raw is None:
        return RecoveryConfigAppliedEvidence()
    if not isinstance(raw, Mapping) or set(raw) != {
        "schema",
        "symbol",
        "next_observation_seq",
        "events",
        "latest",
    }:
        raise _config_applied_fail("config-applied journal envelope is invalid")
    if (
        raw.get("schema") != RECOVERY_CONFIG_APPLIED_RECEIPTS_SCHEMA
        or raw.get("symbol") != recovery_state.symbol
    ):
        raise _config_applied_fail("config-applied journal identity is invalid")
    events = raw.get("events")
    latest = raw.get("latest")
    next_seq = raw.get("next_observation_seq")
    if not (
        isinstance(events, list)
        and isinstance(latest, Mapping)
        and type(next_seq) is int
        and next_seq >= 0
    ):
        raise _config_applied_fail("config-applied journal collections are invalid")

    expected_fields = {
        "receipt_id",
        "kind",
        *binding.keys(),
        "runner_instance_id",
        "config_applied",
        "observed_at",
        "observation_seq",
    }
    previous_seq = -1
    previous_observed_at: datetime | None = None
    seen_receipt_ids: set[str] = set()
    expected_latest: dict[str, str] = {}
    current_receipt: dict[str, Any] | None = None
    current_observed_at: datetime | None = None
    for index, raw_event in enumerate(events):
        path = f"journal.events[{index}]"
        if not isinstance(raw_event, Mapping) or set(raw_event) != expected_fields:
            raise _config_applied_fail(f"{path} fields are invalid")
        event = dict(raw_event)
        if event.get("kind") != "config_applied" or event.get(
            "config_applied"
        ) is not True:
            raise _config_applied_fail(f"{path} is not config-applied proof")
        runner_instance_id = event.get("runner_instance_id")
        if (
            not isinstance(runner_instance_id, str)
            or not runner_instance_id
            or runner_instance_id.strip() != runner_instance_id
        ):
            raise _config_applied_fail(f"{path}.runner_instance_id is invalid")
        event_generation = event.get("generation")
        if (
            event.get("symbol") != recovery_state.symbol
            or type(event_generation) is not int
            or event_generation < 0
            or not isinstance(event.get("decision_id"), str)
            or not str(event.get("decision_id")).strip()
            or not isinstance(event.get("profile_digest"), str)
            or not str(event.get("profile_digest")).strip()
        ):
            raise _config_applied_fail(f"{path} binding is invalid")
        receipt_id = event.get("receipt_id")
        event_binding = {key: event.get(key) for key in binding}
        expected_id = _config_applied_receipt_id(
            binding=event_binding,
            runner_instance_id=runner_instance_id,
        )
        if (
            receipt_id != expected_id
            or not isinstance(receipt_id, str)
            or not re.fullmatch(r"rca-[0-9a-f]{32}", receipt_id)
            or receipt_id in seen_receipt_ids
        ):
            raise _config_applied_fail(f"{path}.receipt_id is invalid")
        seen_receipt_ids.add(receipt_id)
        seq = event.get("observation_seq")
        if type(seq) is not int or seq <= previous_seq:
            raise _config_applied_fail(
                "config-applied receipt sequence is not increasing"
            )
        previous_seq = seq
        expected_latest["config_applied"] = receipt_id
        observed_at = _config_applied_datetime(
            event.get("observed_at"), path=f"{path}.observed_at"
        )
        if observed_at > captured_at:
            raise _config_applied_fail(
                "config-applied receipt is newer than its snapshot"
            )
        if previous_observed_at is not None and observed_at < previous_observed_at:
            raise _config_applied_fail(
                "config-applied receipt time is not monotonic"
            )
        previous_observed_at = observed_at
        if event_generation < binding["generation"]:
            continue
        if event_generation > binding["generation"]:
            raise _config_applied_fail(
                "config-applied receipt is from a future generation"
            )
        if event.get("decision_id") != binding["decision_id"]:
            raise _config_applied_fail(
                "current config-applied generation has a different decision"
            )
        for key, expected in binding.items():
            if event.get(key) != expected:
                raise _config_applied_fail(
                    f"config-applied receipt fence mismatch: {key}"
                )
        if (
            observed_at < recovery_state.issued_at
            or (
                recovery_state.progress_deadline_at is not None
                and observed_at >= recovery_state.progress_deadline_at
            )
            or (
                recovery_state.hard_expires_at is not None
                and observed_at >= recovery_state.hard_expires_at
            )
        ):
            raise _config_applied_fail(
                "config-applied receipt is outside the current action window"
            )
        current_receipt = event
        current_observed_at = observed_at
    if events and next_seq != previous_seq + 1:
        raise _config_applied_fail(
            "config-applied receipt next sequence is not contiguous"
        )
    if not events and next_seq != 0:
        raise _config_applied_fail(
            "empty config-applied journal has non-zero sequence"
        )
    if dict(latest) != expected_latest:
        raise _config_applied_fail(
            "config-applied receipt latest index is invalid"
        )
    if current_receipt is None:
        return RecoveryConfigAppliedEvidence(
            latest_observation_seq=previous_seq,
        )
    return RecoveryConfigAppliedEvidence(
        applied_generation=int(current_receipt["generation"]),
        applied_profile_digest=str(current_receipt["profile_digest"]),
        receipt_id=str(current_receipt["receipt_id"]),
        runner_instance_id=str(current_receipt["runner_instance_id"]),
        observed_at=current_observed_at,
        latest_observation_seq=previous_seq,
    )


def _inventory_fail(message: str) -> InventoryRecoveryReceiptJournalError:
    return InventoryRecoveryReceiptJournalError(message)


def _inventory_datetime(value: Any, *, path: str) -> datetime:
    if not isinstance(value, str):
        raise _inventory_fail(f"{path} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise _inventory_fail(f"{path} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _inventory_fail(f"{path} must be timezone-aware")
    return parsed


def _inventory_string_list(
    value: Any,
    *,
    path: str,
    require_sorted: bool = True,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise _inventory_fail(f"{path} must be a list")
    if any(not isinstance(item, str) for item in value):
        raise _inventory_fail(f"{path} contains a non-string identity")
    rows = tuple(value)
    if any(not item or item.strip() != item for item in rows):
        raise _inventory_fail(f"{path} contains a non-canonical identity")
    if len(rows) != len(set(rows)) or (
        require_sorted and list(rows) != sorted(rows)
    ):
        raise _inventory_fail(
            f"{path} must be unique"
            + (" and sorted" if require_sorted else "")
        )
    return rows


def _ordinary_recovery_binding(state: RecoveryState) -> dict[str, Any]:
    role_by_action = {
        (ActionId.INVENTORY_RECOVER, OrderRole.REDUCE_ONLY, Side.SELL): (
            "best_quote_reduce_long"
        ),
        (ActionId.INVENTORY_RECOVER, OrderRole.REDUCE_ONLY, Side.BUY): (
            "best_quote_reduce_short"
        ),
        (ActionId.MAKER_FLOW_RECOVER, OrderRole.ENTRY, Side.BUY): (
            "best_quote_entry_long"
        ),
        (ActionId.MAKER_FLOW_RECOVER, OrderRole.ENTRY, Side.SELL): (
            "best_quote_entry_short"
        ),
        (ActionId.BASELINE_TUNE, OrderRole.ENTRY, Side.BUY): (
            "best_quote_entry_long"
        ),
        (ActionId.BASELINE_TUNE, OrderRole.ENTRY, Side.SELL): (
            "best_quote_entry_short"
        ),
        (ActionId.BASELINE_TUNE, OrderRole.REDUCE_ONLY, Side.SELL): (
            "best_quote_reduce_long"
        ),
        (ActionId.BASELINE_TUNE, OrderRole.REDUCE_ONLY, Side.BUY): (
            "best_quote_reduce_short"
        ),
    }
    if not (
        state.phase is RecoveryPhase.ACTIVE
        and state.active_action
        in {
            ActionId.INVENTORY_RECOVER,
            ActionId.MAKER_FLOW_RECOVER,
            ActionId.BASELINE_TUNE,
        }
        and state.decision_id
        and state.side is not None
        and state.order_role in {OrderRole.ENTRY, OrderRole.REDUCE_ONLY}
        and (state.active_action, state.order_role, state.side) in role_by_action
        and state.action_lease is None
        and state.desired_profile.fields.get("recovery_order_side")
        == state.side.value
        and state.desired_profile.fields.get("recovery_order_role")
        == state.order_role.value
        and state.desired_profile.fields.get("recovery_order_ledger_class")
        == LedgerClass.NORMAL_BQ.value
    ):
        raise _inventory_fail("recovery state is not an active ordinary recovery")
    role = role_by_action[(state.active_action, state.order_role, state.side)]
    return {
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": str(state.decision_id),
        "profile_digest": state.desired_profile.digest,
        "action_id": state.active_action.value,
        "side": state.side.value,
        "role": role,
        "order_role": state.order_role.value,
        "ledger_class": LedgerClass.NORMAL_BQ.value,
    }


def _inventory_receipt_id(
    *,
    binding: Mapping[str, Any],
    order_ids: tuple[str, ...],
    client_order_ids: tuple[str, ...],
) -> str:
    encoded = json.dumps(
        {
            "kind": "progress",
            "binding": dict(binding),
            "identity": {
                "accepted_order_ids": list(order_ids),
                "accepted_client_order_ids": list(client_order_ids),
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return "irr-" + hashlib.sha256(encoded).hexdigest()[:32]


def load_ordinary_recovery_runtime_evidence(
    *,
    state_path: str | Path,
    recovery_state: RecoveryState,
    captured_at: datetime,
) -> OrdinaryRecoveryRuntimeEvidence:
    """Load a fenced LIMIT+GTX submit manifest, never a fill receipt.

    Acceptance is useful for order ownership and submit liveness, but actual
    action progress comes from side-scoped exchange ``userTrades`` evidence.
    """

    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    binding = _ordinary_recovery_binding(recovery_state)
    expected_client_prefix = ordinary_recovery_client_order_prefix(
        symbol=str(binding["symbol"]),
        generation=int(binding["generation"]),
        decision_id=str(binding["decision_id"]),
        profile_digest=str(binding["profile_digest"]),
        action_id=str(binding["action_id"]),
        side=str(binding["side"]),
        order_role=str(binding["order_role"]),
    )
    try:
        document = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return OrdinaryRecoveryRuntimeEvidence()
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _inventory_fail("runner state document is unreadable") from exc
    if not isinstance(document, Mapping):
        raise _inventory_fail("runner state document must be an object")
    raw = document.get(INVENTORY_RECOVERY_RUNNER_RECEIPTS_KEY)
    if raw is None:
        return OrdinaryRecoveryRuntimeEvidence()
    if not isinstance(raw, Mapping) or set(raw) != {
        "schema",
        "symbol",
        "next_observation_seq",
        "events",
        "latest",
    }:
        raise _inventory_fail("inventory receipt journal envelope is invalid")
    if (
        raw.get("schema") != INVENTORY_RECOVERY_RUNNER_RECEIPTS_SCHEMA
        or raw.get("symbol") != recovery_state.symbol
    ):
        raise _inventory_fail("inventory receipt journal identity is invalid")
    events = raw.get("events")
    latest = raw.get("latest")
    next_seq = raw.get("next_observation_seq")
    if not (
        isinstance(events, list)
        and isinstance(latest, Mapping)
        and type(next_seq) is int
        and next_seq >= 0
    ):
        raise _inventory_fail("inventory receipt journal collections are invalid")

    expected_fields = {
        "receipt_id",
        "kind",
        *binding.keys(),
        "observed_at",
        "observation_seq",
        "accepted_order_ids",
        "accepted_client_order_ids",
        "accepted_statuses",
        "accepted_order_types",
        "accepted_time_in_force",
        "allow_loss_effective",
        "progress_stage",
    }
    previous_seq = -1
    seen_receipt_ids: set[str] = set()
    expected_latest: dict[str, str] = {}
    accepted_order_ids: set[str] = set()
    accepted_client_ids: set[str] = set()
    accepted_order_pairs: set[tuple[str, str]] = set()
    latest_relevant_seq = -1
    for index, raw_event in enumerate(events):
        path = f"journal.events[{index}]"
        if not isinstance(raw_event, Mapping) or set(raw_event) != expected_fields:
            raise _inventory_fail(f"{path} fields are invalid")
        event = dict(raw_event)
        if event.get("kind") != "progress":
            raise _inventory_fail(f"{path}.kind is invalid")
        receipt_id = event.get("receipt_id")
        if (
            not isinstance(receipt_id, str)
            or not re.fullmatch(r"irr-[0-9a-f]{32}", receipt_id)
            or receipt_id in seen_receipt_ids
        ):
            raise _inventory_fail(f"{path}.receipt_id is invalid")
        seen_receipt_ids.add(receipt_id)
        seq = event.get("observation_seq")
        if type(seq) is not int or seq <= previous_seq:
            raise _inventory_fail("inventory receipt sequence is not increasing")
        previous_seq = seq
        expected_latest["progress"] = receipt_id
        observed_at = _inventory_datetime(
            event.get("observed_at"), path=f"{path}.observed_at"
        )
        order_ids = _inventory_string_list(
            event.get("accepted_order_ids"),
            path=f"{path}.accepted_order_ids",
        )
        client_ids = _inventory_string_list(
            event.get("accepted_client_order_ids"),
            path=f"{path}.accepted_client_order_ids",
            require_sorted=False,
        )
        statuses = _inventory_string_list(
            event.get("accepted_statuses"), path=f"{path}.accepted_statuses"
        )
        if (
            not order_ids
            or len(order_ids) != len(client_ids)
            or tuple(zip(order_ids, client_ids, strict=True))
            != tuple(sorted(set(zip(order_ids, client_ids, strict=True))))
            or not statuses
            or not set(statuses).issubset({"NEW", "PARTIALLY_FILLED", "FILLED"})
            or event.get("accepted_order_types") != ["LIMIT"]
            or event.get("accepted_time_in_force") != ["GTX"]
            or event.get("progress_stage") != "gtx_order_accepted"
            or event.get("allow_loss_effective") is not False
        ):
            raise _inventory_fail(f"{path} is not accepted GTX progress")
        event_binding = {key: event.get(key) for key in binding}
        expected_id = _inventory_receipt_id(
            binding=event_binding,
            order_ids=order_ids,
            client_order_ids=client_ids,
        )
        if receipt_id != expected_id:
            raise _inventory_fail(f"{path}.receipt_id does not authenticate binding")
        same_decision = event.get("decision_id") == binding["decision_id"]
        same_generation = event.get("generation") == binding["generation"]
        if same_decision != same_generation:
            raise _inventory_fail("inventory receipt partially matches current action")
        if not same_decision:
            continue
        for key, expected in binding.items():
            if event.get(key) != expected:
                raise _inventory_fail(f"inventory receipt fence mismatch: {key}")
        if (
            observed_at > captured_at
            or recovery_state.issued_at is None
            or observed_at < recovery_state.issued_at
            or (
                recovery_state.progress_deadline_at is not None
                and observed_at >= recovery_state.progress_deadline_at
            )
            or (
                recovery_state.hard_expires_at is not None
                and observed_at >= recovery_state.hard_expires_at
            )
        ):
            raise _inventory_fail("inventory progress receipt is outside action window")
        if accepted_order_ids.intersection(order_ids) or accepted_client_ids.intersection(
            client_ids
        ):
            raise _inventory_fail("inventory progress order identity is duplicated")
        accepted_order_ids.update(order_ids)
        accepted_client_ids.update(client_ids)
        accepted_order_pairs.update(zip(order_ids, client_ids, strict=True))
        latest_relevant_seq = seq
    if events and next_seq != previous_seq + 1:
        raise _inventory_fail("inventory receipt next sequence is not contiguous")
    if not events and next_seq != 0:
        raise _inventory_fail("empty inventory journal has non-zero sequence")
    if dict(latest) != expected_latest:
        raise _inventory_fail("inventory receipt latest index is invalid")
    if any(
        not client_order_id.startswith(expected_client_prefix)
        for _order_id, client_order_id in accepted_order_pairs
    ):
        raise _inventory_fail(
            "ordinary manifest is outside its decision-bound client order prefix"
        )
    return OrdinaryRecoveryRuntimeEvidence(
        progress_receipts=(),
        accepted_order_ids=tuple(sorted(accepted_order_ids)),
        accepted_client_order_ids=tuple(sorted(accepted_client_ids)),
        accepted_order_pairs=tuple(sorted(accepted_order_pairs)),
        latest_observation_seq=latest_relevant_seq,
    )


def _manifest_fill_progress_receipts(
    *,
    recovery_state: RecoveryState,
    accepted_order_ids: tuple[str, ...],
    accepted_client_order_ids: tuple[str, ...],
    accepted_order_pairs: tuple[tuple[str, str], ...],
    trade_rows: Iterable[Mapping[str, Any]],
    captured_at: datetime,
    not_before: datetime,
) -> tuple[ProgressReceipt, ...]:
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("captured_at must be timezone-aware")
    pairs = accepted_order_pairs
    if not pairs:
        return ()
    if (
        pairs != tuple(sorted(set(pairs)))
        or any(not order_id or not client_order_id for order_id, client_order_id in pairs)
        or len({order_id for order_id, _client_order_id in pairs}) != len(pairs)
        or len({_client_order_id for _order_id, _client_order_id in pairs})
        != len(pairs)
        or accepted_order_ids
        != tuple(sorted(order_id for order_id, _client_order_id in pairs))
        or accepted_client_order_ids
        != tuple(sorted(client_order_id for _order_id, client_order_id in pairs))
    ):
        raise ValueError("recovery manifest pairs are inconsistent")
    if recovery_state.issued_at is None:
        raise ValueError("recovery action has no issued_at")

    manifest = dict(pairs)
    expected_side = recovery_state.side
    expected_role = recovery_state.order_role
    assert expected_side is not None
    assert expected_role is not None
    expected_position_side = (
        "LONG"
        if (
            (expected_role is OrderRole.ENTRY and expected_side is Side.BUY)
            or (
                expected_role is OrderRole.REDUCE_ONLY
                and expected_side is Side.SELL
            )
        )
        else "SHORT"
    )
    latest: tuple[datetime, int] | None = None
    for raw in trade_rows:
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("symbol") or "").upper().strip() != recovery_state.symbol:
            continue
        order_id = str(raw.get("orderId") or raw.get("order_id") or "").strip()
        expected_client_order_id = manifest.get(order_id)
        if expected_client_order_id is None:
            continue
        client_order_id = str(
            raw.get("clientOrderId") or raw.get("client_order_id") or ""
        ).strip()
        if client_order_id and client_order_id != expected_client_order_id:
            continue
        if str(raw.get("side") or "").upper().strip() != expected_side.value:
            continue
        if (
            str(raw.get("positionSide") or raw.get("position_side") or "")
            .upper()
            .strip()
            != expected_position_side
        ):
            continue
        raw_quantity = raw.get("qty")
        if isinstance(raw_quantity, bool):
            continue
        try:
            quantity = float(raw_quantity)
        except (TypeError, ValueError):
            continue
        if not isfinite(quantity) or quantity <= 0:
            continue
        observed_at = None
        for field in ("time", "transactTime"):
            try:
                milliseconds = int(float(raw.get(field)))
                if milliseconds > 0:
                    observed_at = datetime.fromtimestamp(
                        milliseconds / 1000.0,
                        tz=timezone.utc,
                    )
                    break
            except (OSError, OverflowError, TypeError, ValueError):
                continue
        if (
            observed_at is None
            or observed_at < max(recovery_state.issued_at, not_before)
            or observed_at > captured_at
            or (
                recovery_state.progress_deadline_at is not None
                and observed_at >= recovery_state.progress_deadline_at
            )
            or (
                recovery_state.hard_expires_at is not None
                and observed_at >= recovery_state.hard_expires_at
            )
        ):
            continue
        raw_sequence = raw.get("id")
        if type(raw_sequence) is int:
            observation_seq = raw_sequence
        elif (
            isinstance(raw_sequence, str)
            and raw_sequence
            and raw_sequence.isdigit()
        ):
            observation_seq = int(raw_sequence)
        else:
            continue
        if observation_seq < 0:
            continue
        candidate = (observed_at, observation_seq)
        if latest is None or candidate > latest:
            latest = candidate
    if latest is None:
        return ()
    observed_at, observation_seq = latest
    return (
        ProgressReceipt(
            decision_id=str(recovery_state.decision_id),
            generation=recovery_state.generation,
            side=expected_side,
            order_role=expected_role,
            observation_seq=observation_seq,
            observed_at=observed_at,
        ),
    )


def ordinary_recovery_fill_progress_receipts(
    *,
    recovery_state: RecoveryState,
    runtime_evidence: OrdinaryRecoveryRuntimeEvidence,
    trade_rows: Iterable[Mapping[str, Any]],
    captured_at: datetime,
) -> tuple[ProgressReceipt, ...]:
    """Return progress only for a current ordinary manifest userTrade fill."""

    _ordinary_recovery_binding(recovery_state)
    return _manifest_fill_progress_receipts(
        recovery_state=recovery_state,
        accepted_order_ids=runtime_evidence.accepted_order_ids,
        accepted_client_order_ids=runtime_evidence.accepted_client_order_ids,
        accepted_order_pairs=runtime_evidence.accepted_order_pairs,
        trade_rows=trade_rows,
        captured_at=captured_at,
        not_before=recovery_state.issued_at,
    )


def temporary_loss_fill_progress_receipts(
    *,
    recovery_state: RecoveryState,
    runtime_evidence: TemporaryLossRuntimeEvidence,
    trade_rows: Iterable[Mapping[str, Any]],
    captured_at: datetime,
) -> tuple[ProgressReceipt, ...]:
    """Return progress only for a current temporary-loss manifest fill."""

    lease = recovery_state.action_lease
    if not (
        recovery_state.phase is RecoveryPhase.ACTIVE
        and lease is not None
        and lease.activated_at is not None
    ):
        raise _fail("temporary-loss fill requires one active lease")
    _current_binding(recovery_state)
    return _manifest_fill_progress_receipts(
        recovery_state=recovery_state,
        accepted_order_ids=runtime_evidence.accepted_order_ids,
        accepted_client_order_ids=runtime_evidence.accepted_client_order_ids,
        accepted_order_pairs=runtime_evidence.accepted_order_pairs,
        trade_rows=trade_rows,
        captured_at=captured_at,
        not_before=lease.activated_at,
    )


def load_inventory_recovery_runtime_evidence(
    *,
    state_path: str | Path,
    recovery_state: RecoveryState,
    captured_at: datetime,
) -> InventoryRecoveryRuntimeEvidence:
    """Compatibility wrapper restricted to INVENTORY_RECOVER."""

    if recovery_state.active_action is not ActionId.INVENTORY_RECOVER:
        raise _inventory_fail("recovery state is not active inventory recovery")
    return load_ordinary_recovery_runtime_evidence(
        state_path=state_path,
        recovery_state=recovery_state,
        captured_at=captured_at,
    )


def _open_orders_by_id(
    rows: Any,
    *,
    symbol: str,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(rows, list):
        raise _fail("exchange open-orders snapshot must be a list")
    result: dict[str, Mapping[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise _fail(f"exchange open-orders[{index}] must be an object")
        if str(row.get("symbol") or "").upper().strip() != symbol:
            raise _fail(f"exchange open-orders[{index}] symbol mismatch")
        order_id = str(row.get("orderId") or "").strip()
        if not order_id or not order_id.isdigit():
            raise _fail(f"exchange open-orders[{index}] order id is invalid")
        if order_id in result:
            raise _fail("exchange open-orders snapshot contains duplicate order ids")
        result[order_id] = row
    return result


def _validate_owned_gtx_order(
    row: Mapping[str, Any],
    *,
    manifest: ManagedOrderManifest,
    identity: ManagedOrderIdentity,
) -> None:
    expected_position_side = (
        "LONG"
        if (
            manifest.order_role is OrderRole.ENTRY
            and manifest.side is Side.BUY
        )
        or (
            manifest.order_role is OrderRole.REDUCE_ONLY
            and manifest.side is Side.SELL
        )
        else "SHORT"
    )
    if not (
        str(row.get("orderId") or "").strip() == identity.order_id
        and str(row.get("clientOrderId") or "").strip()
        == identity.client_order_id
        and str(row.get("type") or "").upper().strip() == "LIMIT"
        and str(row.get("timeInForce") or "").upper().strip() == "GTX"
        and str(row.get("side") or "").upper().strip()
        == manifest.side.value
        and str(row.get("positionSide") or "").upper().strip()
        == expected_position_side
    ):
        raise _fail(
            f"owned order {identity.order_id} clientOrderId or maker-only GTX scope mismatch"
        )


def observe_temporary_loss_open_order_ids(
    *,
    recovery_state: RecoveryState,
    evidence: TemporaryLossRuntimeEvidence,
    fetch_open_orders: Any,
) -> tuple[str, ...]:
    """Return the fresh exchange-open subset of one authenticated manifest."""

    if not evidence.cleanup_requested:
        return ()
    if recovery_state.phase is not RecoveryPhase.ACTIVE:
        raise _fail("cleanup-needed evidence requires an active temporary-loss lease")
    if not evidence.cleanup_order_ids or (
        len(evidence.cleanup_order_ids) != len(evidence.cleanup_client_order_ids)
    ):
        raise _fail("cleanup-needed evidence has no complete exchange order manifest")
    open_by_id = _open_orders_by_id(
        fetch_open_orders(recovery_state.symbol),
        symbol=recovery_state.symbol,
    )
    if recovery_state.decision_id is None or recovery_state.issued_at is None:
        raise _fail("temporary-loss cleanup binding is incomplete")
    pairs = tuple(
        sorted(
            ManagedOrderIdentity(order_id=order_id, client_order_id=client_id)
            for order_id, client_id in evidence.accepted_order_pairs
        )
    )
    manifest = ManagedOrderManifest(
        symbol=recovery_state.symbol,
        generation=recovery_state.generation,
        decision_id=recovery_state.decision_id,
        profile_digest=recovery_state.desired_profile.digest,
        action_id=recovery_state.active_action,
        side=recovery_state.side,
        order_role=recovery_state.order_role,
        issued_at=recovery_state.issued_at,
        orders=pairs,
        action_lease_epoch=recovery_state.action_lease.epoch,
        hard_expires_at=recovery_state.action_lease.hard_expires_at,
    )
    result: list[str] = []
    for identity in manifest.orders:
        row = open_by_id.get(identity.order_id)
        if row is None:
            continue
        _validate_owned_gtx_order(
            row,
            manifest=manifest,
            identity=identity,
        )
        result.append(identity.order_id)
    return tuple(result)


def _user_trades_watermark(rows: Any, *, owned_order_ids: set[str]) -> int:
    if not isinstance(rows, list):
        raise _fail("exchange user-trades snapshot must be a list")
    watermark = 0
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise _fail(f"exchange user-trades[{index}] must be an object")
        order_id = str(row.get("orderId") or "").strip()
        if order_id not in owned_order_ids:
            continue
        trade_id = row.get("id")
        if type(trade_id) is int and trade_id >= 0:
            normalized_trade_id = trade_id
        elif isinstance(trade_id, str) and trade_id.isdigit():
            normalized_trade_id = int(trade_id)
        else:
            raise _fail(f"exchange user-trades[{index}] trade id is invalid")
        watermark = max(watermark, normalized_trade_id)
    return watermark


def _cancel_error_is_explicitly_already_terminal(exc: Exception) -> bool:
    message = str(exc).lower()
    return bool(
        "-2011" in message
        and (
            "unknown order" in message
            or "order does not exist" in message
            or "already terminal" in message
        )
    )


def _managed_cleanup_rebuild_prefix(manifest: ManagedOrderManifest) -> str:
    if manifest.action_id is ActionId.TEMPORARY_LOSS_RELIEF:
        if manifest.action_lease_epoch is None or manifest.hard_expires_at is None:
            raise _fail("temporary-loss cleanup prefix binding is incomplete")
        return temporary_loss_client_order_prefix_for_binding(
            symbol=manifest.symbol,
            source_decision_id=manifest.decision_id,
            action_lease_epoch=manifest.action_lease_epoch,
            side=manifest.side,
            hard_expires_at=manifest.hard_expires_at,
        )
    return ordinary_recovery_client_order_prefix(
        symbol=manifest.symbol,
        generation=manifest.generation,
        decision_id=manifest.decision_id,
        profile_digest=manifest.profile_digest,
        action_id=manifest.action_id.value,
        side=manifest.side.value,
        order_role=manifest.order_role.value,
    )


def reconcile_managed_gtx_cleanup(
    *,
    recovery_state: RecoveryState,
    observed_at: datetime,
    fetch_open_orders: Any,
    cancel_order: Any,
    fetch_user_trades_page: Any,
    trade_page_limit: int = 1000,
    max_trade_page_requests: int = 80,
) -> ManagedGtxCleanupResult:
    """Cancel only one decision-bound manifest and build a fresh proof.

    All exchange functions are injected so callers can use the project's
    authenticated data adapter.  The complete pre-cancel snapshot is fenced
    before the first cancellation.  An unrelated strategy order is observed
    but never canceled.
    """

    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    obligation = recovery_state.cleanup_obligation
    if not (
        recovery_state.phase is RecoveryPhase.CLEANING
        and recovery_state.decision_id
        and obligation is not None
        and obligation.source_decision_id
        and recovery_state.pending_effect_stage
        in {EffectStage.MANAGED_GTX_CANCEL, EffectStage.LOCAL_STATE_REPAIR}
        and recovery_state.pending_effect_epoch is not None
    ):
        raise _fail("recovery state is not an executable managed GTX cleanup")
    if observed_at < obligation.created_at:
        raise _fail("cleanup observation predates its obligation")
    manifest = obligation.managed_order_manifest
    if manifest.symbol != recovery_state.symbol:
        raise _fail("cleanup manifest symbol mismatch")
    if recovery_state.pending_effect_stage is EffectStage.MANAGED_GTX_CANCEL:
        if not manifest.orders or obligation.needs_manifest_rebuild:
            raise _fail("managed GTX cancel requires a complete non-empty manifest")
    elif manifest.orders or not obligation.needs_manifest_rebuild:
        raise _fail("local state repair is valid only for an observed empty manifest")

    rebuild_prefix = _managed_cleanup_rebuild_prefix(manifest)
    if any(
        not identity.client_order_id.startswith(rebuild_prefix)
        for identity in manifest.orders
    ):
        raise _fail(
            "managed manifest is outside its decision-bound client order prefix"
        )
    before = _open_orders_by_id(
        fetch_open_orders(recovery_state.symbol),
        symbol=recovery_state.symbol,
    )
    # The audit ring is intentionally bounded, so even a non-empty persisted
    # manifest may omit an older order from the same decision.  Supplement it
    # with the decision-bound prefix on every cleanup; never widen to the
    # symbol-wide strategy prefix.
    owned_by_id = {item.order_id: item for item in manifest.orders}
    for order_id, row in before.items():
        client_order_id = str(row.get("clientOrderId") or "").strip()
        if not client_order_id.startswith(rebuild_prefix):
            continue
        discovered = ManagedOrderIdentity(
            order_id=order_id,
            client_order_id=client_order_id,
        )
        previous = owned_by_id.get(order_id)
        if previous is not None and previous != discovered:
            raise _fail(
                f"owned order {order_id} clientOrderId manifest mismatch"
            )
        owned_by_id[order_id] = discovered
    owned = tuple(sorted(owned_by_id.values(), key=lambda item: int(item.order_id)))
    for identity in owned:
        row = before.get(identity.order_id)
        if row is not None:
            _validate_owned_gtx_order(
                row,
                manifest=manifest,
                identity=identity,
            )

    canceled: list[str] = []
    already_terminal: list[str] = []
    for identity in owned:
        order_id = identity.order_id
        if order_id not in before:
            continue
        try:
            response = cancel_order(recovery_state.symbol, order_id)
        except RuntimeError as exc:
            if _cancel_error_is_explicitly_already_terminal(exc):
                already_terminal.append(order_id)
                continue
            raise
        if not isinstance(response, Mapping):
            raise _fail(f"cancel response for owned order {order_id} is invalid")
        if (
            str(response.get("symbol") or "").upper().strip()
            != recovery_state.symbol
            or str(response.get("orderId") or "").strip() != order_id
        ):
            raise _fail(f"cancel response for owned order {order_id} is not exact")
        status = str(response.get("status") or "").upper().strip()
        if status == "CANCELED":
            canceled.append(order_id)
        elif status in {"FILLED", "EXPIRED", "EXPIRED_IN_MATCH"}:
            already_terminal.append(order_id)
        else:
            raise _fail(f"cancel response for owned order {order_id} is not terminal")

    after = _open_orders_by_id(
        fetch_open_orders(recovery_state.symbol),
        symbol=recovery_state.symbol,
    )
    remaining_by_id = {
        identity.order_id: identity
        for identity in owned
        if identity.order_id in after
    }
    remaining_by_id.update(
        {
            order_id: ManagedOrderIdentity(
                order_id=order_id,
                client_order_id=str(row.get("clientOrderId") or "").strip(),
            )
            for order_id, row in after.items()
            if str(row.get("clientOrderId") or "").startswith(rebuild_prefix)
        }
    )
    remaining = tuple(sorted(remaining_by_id, key=int))
    for order_id, identity in remaining_by_id.items():
        _validate_owned_gtx_order(
            after[order_id],
            manifest=manifest,
            identity=identity,
        )
    if remaining:
        return ManagedGtxCleanupResult(
            canceled_order_ids=tuple(canceled),
            already_terminal_order_ids=tuple(already_terminal),
            remaining_order_ids=remaining,
            blocked_reason="owned_gtx_orders_still_open",
        )

    start_time_ms = int(manifest.issued_at.timestamp() * 1000)
    end_time_ms = int(observed_at.timestamp() * 1000)
    try:
        exact_trades = fetch_exact_futures_trade_window(
            fetch_page=lambda start_ms, inclusive_end_ms, limit: (
                fetch_user_trades_page(
                    recovery_state.symbol,
                    start_ms,
                    inclusive_end_ms,
                    limit,
                )
            ),
            start_time_ms=start_time_ms,
            end_time_exclusive_ms=end_time_ms,
            page_limit=trade_page_limit,
            max_requests=max_trade_page_requests,
        )
    except (TypeError, ValueError) as exc:
        raise _fail(f"exact user-trades window is incomplete: {exc}") from exc
    watermark = _user_trades_watermark(
        exact_trades,
        owned_order_ids={identity.order_id for identity in owned},
    )
    effect_receipt = EffectReceipt(
        decision_id=str(recovery_state.decision_id),
        stage=recovery_state.pending_effect_stage,
        effect_epoch=int(recovery_state.pending_effect_epoch),
        observed_at=observed_at,
    )
    proof = CleanupProof(
        source_decision_id=obligation.source_decision_id,
        source_generation=manifest.generation,
        manifest_digest=manifest.digest,
        remaining_order_ids=(),
        user_trades_watermark=watermark,
        observed_at=observed_at,
    )
    return ManagedGtxCleanupResult(
        canceled_order_ids=tuple(canceled),
        already_terminal_order_ids=tuple(already_terminal),
        remaining_order_ids=(),
        effect_receipt=effect_receipt,
        cleanup_proof=proof,
    )


def reconcile_temporary_loss_cleanup(
    *,
    recovery_state: RecoveryState,
    observed_at: datetime,
    fetch_open_orders: Any,
    cancel_order: Any,
    fetch_user_trades_page: Any,
    trade_page_limit: int = 1000,
    max_trade_page_requests: int = 80,
) -> ManagedGtxCleanupResult:
    """Compatibility wrapper for the generalized managed-order cleanup."""

    return reconcile_managed_gtx_cleanup(
        recovery_state=recovery_state,
        observed_at=observed_at,
        fetch_open_orders=fetch_open_orders,
        cancel_order=cancel_order,
        fetch_user_trades_page=fetch_user_trades_page,
        trade_page_limit=trade_page_limit,
        max_trade_page_requests=max_trade_page_requests,
    )
