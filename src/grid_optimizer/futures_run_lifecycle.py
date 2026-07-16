"""Pure, symbol-scoped lifecycle decisions for predictable futures runs.

The lifecycle owns *when* a run may leave its target-seeking phase.  Recovery
and terminal-drain implementations remain separate executors, but they are
represented here as one top-level intent per decision.  In particular, a
deadline is not a stop signal: it starts a terminal drain, and only a fresh,
reconciled flat proof may produce a normal stop.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Mapping


RUN_CONTRACT_OWNER_KEY = "futures_run_contract_owner"
RUN_CONTRACT_OWNER_SCHEMA = "futures_run_contract_owner_v1"


class RunPhase(str, Enum):
    RUNNING = "running"
    RECOVERING = "recovering"
    TERMINAL_DRAIN = "terminal_drain"
    EXIT_BLOCKED = "exit_blocked"
    STOPPED_CLEAN = "stopped_clean"
    STOPPED_PRESERVED = "stopped_preserved"


class RunOutcome(str, Enum):
    TARGET_REACHED = "target_reached"
    TARGET_UNMET_DEADLINE = "target_unmet_deadline"
    CONDITION_UNMET = "condition_unmet"
    STOP_PRESERVE = "stop_preserve"


class LifecycleActionId(str, Enum):
    CONTINUE_TARGET = "continue_target"
    APPLY_RECOVERY = "apply_recovery"
    START_TERMINAL_DRAIN = "start_terminal_drain"
    RUN_TERMINAL_DRAIN = "run_terminal_drain"
    MARK_EXIT_BLOCKED = "mark_exit_blocked"
    RETRY_TERMINAL_DRAIN = "retry_terminal_drain"
    STOP_CLEAN = "stop_clean"
    STOP_PRESERVE = "stop_preserve"


@dataclass(frozen=True)
class LifecycleIntent:
    """One executor intent selected for the current symbol and cycle."""

    action_id: str
    reason: str

    def __post_init__(self) -> None:
        if not str(self.action_id).strip():
            raise ValueError("intent action_id is required")
        if not str(self.reason).strip():
            raise ValueError("intent reason is required")


@dataclass(frozen=True)
class RecoveryIntent(LifecycleIntent):
    """A recoverable action that should improve progress toward the target."""


@dataclass(frozen=True)
class CleanFlatProof:
    """Evidence required before a normal lifecycle stop is legal."""

    captured_at: datetime
    exchange_long_qty: float
    exchange_short_qty: float
    owned_long_qty: float
    owned_short_qty: float
    open_managed_order_count: int
    ledger_reconciled: bool
    trade_watermark_reconciled: bool
    confirmation_count: int

    def validation_errors(
        self,
        *,
        observed_at: datetime,
        policy: "RunLifecyclePolicy",
    ) -> tuple[str, ...]:
        errors: list[str] = []
        age = observed_at - self.captured_at
        if age < timedelta(0):
            errors.append("flat_proof_from_future")
        elif age > policy.flat_proof_max_age:
            errors.append("flat_proof_stale")
        if abs(float(self.exchange_long_qty)) > policy.flat_tolerance:
            errors.append("exchange_long_nonzero")
        if abs(float(self.exchange_short_qty)) > policy.flat_tolerance:
            errors.append("exchange_short_nonzero")
        if abs(float(self.owned_long_qty)) > policy.flat_tolerance:
            errors.append("owned_long_nonzero")
        if abs(float(self.owned_short_qty)) > policy.flat_tolerance:
            errors.append("owned_short_nonzero")
        if int(self.open_managed_order_count) != 0:
            errors.append("managed_orders_open")
        if not self.ledger_reconciled:
            errors.append("ledger_unreconciled")
        if not self.trade_watermark_reconciled:
            errors.append("trade_watermark_unreconciled")
        if int(self.confirmation_count) < policy.flat_confirmations_required:
            errors.append("flat_confirmations_insufficient")
        return tuple(errors)


@dataclass(frozen=True)
class TerminalDrainFeedback:
    """The terminal-drain owner's one result for this lifecycle cycle."""

    action_id: str | None = None
    blocked_reason: str | None = None
    stop_allowed: bool = False
    flat_proof: CleanFlatProof | None = None

    def __post_init__(self) -> None:
        if self.action_id is not None and not str(self.action_id).strip():
            raise ValueError("terminal drain action_id cannot be blank")
        if self.blocked_reason is not None and not str(self.blocked_reason).strip():
            raise ValueError("terminal drain blocked_reason cannot be blank")
        selected = sum(
            (
                self.action_id is not None,
                self.blocked_reason is not None,
                bool(self.stop_allowed),
            )
        )
        if selected > 1:
            raise ValueError("terminal drain feedback must select only one result")
        if self.flat_proof is not None and not self.stop_allowed:
            raise ValueError("flat_proof is only valid with stop_allowed")


@dataclass(frozen=True)
class RunLifecyclePolicy:
    flat_tolerance: float = 1e-9
    flat_proof_max_age: timedelta = timedelta(seconds=10)
    flat_confirmations_required: int = 2

    def __post_init__(self) -> None:
        if self.flat_tolerance < 0:
            raise ValueError("flat_tolerance must be non-negative")
        if self.flat_proof_max_age.total_seconds() <= 0:
            raise ValueError("flat_proof_max_age must be positive")
        if self.flat_confirmations_required < 1:
            raise ValueError("flat_confirmations_required must be positive")


@dataclass(frozen=True)
class RunContract:
    """Validated, immutable target and exit terms for one strategy run.

    A bounded run must have a finite way out before it is allowed to start.
    Optional wear-exit thresholds are part of the same immutable contract and
    remain disabled unless both the limit and arming notional are explicit.
    ``drain_then_preserve`` first spends only the declared loss budget and then
    preserves any remainder after the declared wait.  ``stop_preserve`` exits
    directly with a recorded operator reason.  An unbounded ``drain_clean``
    contract is retained only for compatibility; it is deliberately illegal
    for deadline/target runs because it can remain in ``EXIT_BLOCKED`` forever.
    """

    run_start_time: datetime | None
    runtime_guard_stats_start_time: datetime | None
    run_end_time: datetime | None
    target_value: float | None
    exit_policy: str | None
    loss_budget: float | None
    max_wait_seconds: float | None
    preserve_reason: str | None
    wear_stop_per_10k: float | None
    wear_stop_min_gross_notional: float | None

    @property
    def bounded(self) -> bool:
        return (
            self.run_end_time is not None
            or self.target_value is not None
            or self.wear_stop_per_10k is not None
        )


def _normalize_contract_time(value: Any, field_name: str) -> datetime | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid ISO timestamp") from exc
    else:
        raise ValueError(f"{field_name} must be a datetime or ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone information")
    return parsed.astimezone(timezone.utc)


def _normalize_contract_deadline(value: Any) -> datetime | None:
    return _normalize_contract_time(value, "run_end_time")


def _normalize_contract_number(value: Any, field_name: str) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be finite")
    if parsed < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return parsed


def validate_run_contract(
    *,
    run_end_time: datetime | str | None,
    target_value: Any,
    exit_policy: str | None,
    loss_budget: Any,
    max_wait_seconds: Any,
    preserve_reason: str | None,
    runtime_guard_stats_start_time: datetime | str | None = None,
    run_start_time: datetime | str | None = None,
    wear_stop_per_10k: Any = None,
    wear_stop_min_gross_notional: Any = None,
) -> RunContract:
    """Validate run termination terms without reading or mutating runtime state."""

    deadline = _normalize_contract_deadline(run_end_time)
    start = _normalize_contract_time(run_start_time, "run_start_time")
    stats_start = _normalize_contract_time(
        runtime_guard_stats_start_time,
        "runtime_guard_stats_start_time",
    )
    target = _normalize_contract_number(target_value, "target_value")
    budget = _normalize_contract_number(loss_budget, "loss_budget")
    max_wait = _normalize_contract_number(max_wait_seconds, "max_wait_seconds")
    wear_stop = _normalize_contract_number(
        wear_stop_per_10k,
        "wear_stop_per_10k",
    )
    wear_min_gross = _normalize_contract_number(
        wear_stop_min_gross_notional,
        "wear_stop_min_gross_notional",
    )
    policy = str(exit_policy).strip() if exit_policy is not None else ""
    normalized_policy = policy or None
    reason = str(preserve_reason).strip() if preserve_reason is not None else ""
    normalized_reason = reason or None

    if target is not None and target <= 0:
        raise ValueError("target_value must be > 0")
    if target is not None and deadline is None:
        raise ValueError("target_value requires run_end_time")
    if target is not None and stats_start is None:
        raise ValueError("target_value requires runtime_guard_stats_start_time")
    if stats_start is not None and deadline is not None and stats_start >= deadline:
        raise ValueError(
            "runtime_guard_stats_start_time must be earlier than run_end_time"
        )
    if start is not None and deadline is not None and start >= deadline:
        raise ValueError("run_start_time must be earlier than run_end_time")
    if wear_stop is None and wear_min_gross is not None:
        raise ValueError(
            "wear_stop_min_gross_notional requires wear_stop_per_10k"
        )
    if wear_stop is not None:
        if stats_start is None:
            raise ValueError("wear_stop_per_10k requires runtime_guard_stats_start_time")
        if deadline is None:
            raise ValueError("wear_stop_per_10k requires run_end_time")
        if wear_min_gross is None:
            raise ValueError(
                "wear_stop_min_gross_notional is required when wear stop is enabled"
            )
        if wear_min_gross <= 0:
            raise ValueError("wear_stop_min_gross_notional must be > 0")

    bounded = deadline is not None or target is not None
    if bounded and normalized_policy is None:
        raise ValueError("bounded run requires exit_policy")

    supported_policies = {"drain_clean", "drain_then_preserve", "stop_preserve"}
    if normalized_policy is not None and normalized_policy not in supported_policies:
        raise ValueError(
            "exit_policy must be drain_clean, drain_then_preserve, or stop_preserve"
        )
    if bounded and normalized_policy not in {"drain_then_preserve", "stop_preserve"}:
        raise ValueError(
            "bounded run exit_policy must be drain_then_preserve or stop_preserve"
        )

    if normalized_policy == "drain_then_preserve":
        if budget is None:
            raise ValueError("loss_budget is required for drain_then_preserve")
        if budget <= 0:
            raise ValueError("loss_budget must be > 0 for drain_then_preserve")
        if max_wait is None:
            raise ValueError("max_wait_seconds is required for drain_then_preserve")
        if max_wait <= 0:
            raise ValueError("max_wait_seconds must be > 0 for drain_then_preserve")
    elif normalized_policy == "stop_preserve":
        if budget is None:
            raise ValueError("loss_budget is required for stop_preserve")
        if normalized_reason is None:
            raise ValueError("preserve_reason is required for stop_preserve")

    return RunContract(
        run_start_time=start,
        runtime_guard_stats_start_time=stats_start,
        run_end_time=deadline,
        target_value=target,
        exit_policy=normalized_policy,
        loss_budget=budget,
        max_wait_seconds=max_wait,
        preserve_reason=normalized_reason,
        wear_stop_per_10k=wear_stop,
        wear_stop_min_gross_notional=wear_min_gross,
    )


def run_contract_snapshot_from_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Return the one canonical, serializable target-and-exit contract."""

    contract = validate_run_contract(
        run_start_time=config.get("run_start_time"),
        runtime_guard_stats_start_time=config.get("runtime_guard_stats_start_time"),
        run_end_time=config.get("run_end_time"),
        target_value=config.get("max_cumulative_notional"),
        exit_policy=config.get("terminal_drain_exit_policy"),
        loss_budget=config.get("terminal_drain_absolute_loss_budget"),
        max_wait_seconds=config.get("terminal_drain_max_wait_seconds"),
        preserve_reason=config.get("terminal_drain_stop_preserve_reason"),
        wear_stop_per_10k=config.get("lifecycle_wear_stop_per_10k"),
        wear_stop_min_gross_notional=config.get(
            "lifecycle_wear_stop_min_gross_notional"
        ),
    )
    explicit_max_order = _normalize_contract_number(
        config.get("terminal_drain_max_order_notional", 0.0),
        "terminal_drain_max_order_notional",
    )
    per_order_fallback = _normalize_contract_number(
        config.get("per_order_notional", 0.0),
        "per_order_notional",
    )
    max_order = explicit_max_order or per_order_fallback
    loss_lease = _normalize_contract_number(
        config.get("terminal_drain_loss_lease_seconds", 300.0),
        "terminal_drain_loss_lease_seconds",
    )
    reprice = _normalize_contract_number(
        config.get("terminal_drain_order_reprice_seconds", 120.0),
        "terminal_drain_order_reprice_seconds",
    )
    flat_confirm = _normalize_contract_number(
        config.get("terminal_drain_flat_confirm_cycles", 2),
        "terminal_drain_flat_confirm_cycles",
    )
    return {
        "schema": "futures_run_contract_snapshot_v3",
        "symbol": str(config.get("symbol") or "").upper().strip(),
        "strategy_profile": str(config.get("strategy_profile") or "").strip(),
        "strategy_mode": str(config.get("strategy_mode") or "").strip(),
        "run_start_time": (
            contract.run_start_time.isoformat()
            if contract.run_start_time is not None
            else None
        ),
        "runtime_guard_stats_start_time": (
            contract.runtime_guard_stats_start_time.isoformat()
            if contract.runtime_guard_stats_start_time is not None
            else None
        ),
        "run_end_time": (
            contract.run_end_time.isoformat()
            if contract.run_end_time is not None
            else None
        ),
        "max_cumulative_notional": contract.target_value,
        "terminal_drain_exit_policy": contract.exit_policy,
        "terminal_drain_absolute_loss_budget": contract.loss_budget,
        "terminal_drain_max_wait_seconds": contract.max_wait_seconds,
        "terminal_drain_stop_preserve_reason": contract.preserve_reason,
        "terminal_drain_max_order_notional": max_order,
        "terminal_drain_loss_lease_seconds": loss_lease,
        "terminal_drain_order_reprice_seconds": reprice,
        "terminal_drain_flat_confirm_cycles": int(flat_confirm or 0),
        "lifecycle_wear_stop_per_10k": contract.wear_stop_per_10k,
        "lifecycle_wear_stop_min_gross_notional": (
            contract.wear_stop_min_gross_notional
        ),
    }


def run_contract_snapshot_digest(config: Mapping[str, Any]) -> str:
    """Return the full digest of the canonical v3 snapshot."""

    payload = run_contract_snapshot_from_config(config)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def run_contract_identity_from_config(config: Mapping[str, Any]) -> str:
    """Hash the canonical target-and-exit contract for one strategy run."""

    digest = run_contract_snapshot_digest(config).removeprefix("sha256:")
    return "futures-run-contract-v3-" + digest[:24]


def _run_contract_owner_digest(owner: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in owner.items() if key != "owner_digest"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _new_run_contract_owner(
    snapshot: Mapping[str, Any],
    *,
    activated_at: datetime,
    generation: int,
    initial_per_order_notional: float,
    handoff_from_contract_id: str | None,
    handoff_reason: str | None,
) -> dict[str, Any]:
    normalized_activated_at = _normalize_contract_time(activated_at, "activated_at")
    if normalized_activated_at is None:  # pragma: no cover - guarded by the type
        raise ValueError("activated_at is required")
    canonical_snapshot = run_contract_snapshot_from_config(snapshot)
    owner: dict[str, Any] = {
        "schema": RUN_CONTRACT_OWNER_SCHEMA,
        "symbol": str(canonical_snapshot.get("symbol") or "").upper().strip(),
        "generation": int(generation),
        "activated_at": normalized_activated_at.isoformat(),
        "run_contract_id": run_contract_identity_from_config(canonical_snapshot),
        "run_contract_digest": run_contract_snapshot_digest(canonical_snapshot),
        "run_contract_snapshot": canonical_snapshot,
        # Audit only.  Recovery may adjust normal order size inside the same
        # run; the immutable terminal cap lives in the canonical snapshot.
        "initial_per_order_notional": float(initial_per_order_notional),
        "handoff_from_contract_id": handoff_from_contract_id,
        "handoff_reason": handoff_reason,
    }
    owner["owner_digest"] = _run_contract_owner_digest(owner)
    return owner


def validate_run_contract_owner(
    owner: Mapping[str, Any],
    *,
    expected_symbol: str | None = None,
) -> dict[str, Any]:
    """Validate a durable bounded-run owner and return its canonical snapshot."""

    if not isinstance(owner, Mapping):
        raise ValueError("run contract owner must be an object")
    if owner.get("schema") != RUN_CONTRACT_OWNER_SCHEMA:
        raise ValueError("run contract owner schema mismatch")
    raw_snapshot = owner.get("run_contract_snapshot")
    if not isinstance(raw_snapshot, Mapping):
        raise ValueError("run contract owner snapshot is missing")
    canonical_snapshot = run_contract_snapshot_from_config(raw_snapshot)
    if dict(raw_snapshot) != canonical_snapshot:
        raise ValueError("run contract owner snapshot is not canonical")
    if canonical_snapshot.get("run_end_time") is None and canonical_snapshot.get(
        "max_cumulative_notional"
    ) is None:
        raise ValueError("run contract owner must bind a bounded run")

    symbol = str(owner.get("symbol") or "").upper().strip()
    snapshot_symbol = str(canonical_snapshot.get("symbol") or "").upper().strip()
    if not symbol or symbol != snapshot_symbol:
        raise ValueError("run contract owner symbol mismatch")
    normalized_expected = str(expected_symbol or "").upper().strip()
    if normalized_expected and symbol != normalized_expected:
        raise ValueError("run contract owner expected symbol mismatch")

    expected_contract_digest = run_contract_snapshot_digest(canonical_snapshot)
    if owner.get("run_contract_digest") != expected_contract_digest:
        raise ValueError("run contract owner digest mismatch")
    expected_contract_id = run_contract_identity_from_config(canonical_snapshot)
    if owner.get("run_contract_id") != expected_contract_id:
        raise ValueError("run contract owner contract id mismatch")

    generation = owner.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise ValueError("run contract owner generation must be a positive integer")
    _normalize_contract_time(owner.get("activated_at"), "activated_at")
    initial_per_order = _normalize_contract_number(
        owner.get("initial_per_order_notional"),
        "initial_per_order_notional",
    )
    if initial_per_order is None:
        raise ValueError("run contract owner initial_per_order_notional is missing")
    predecessor = str(owner.get("handoff_from_contract_id") or "").strip() or None
    reason = str(owner.get("handoff_reason") or "").strip() or None
    if generation == 1 and (predecessor is not None or reason is not None):
        raise ValueError("first run contract owner cannot declare a handoff")
    if generation > 1 and (predecessor is None or reason is None):
        raise ValueError("replacement run contract owner requires handoff evidence")
    if owner.get("owner_digest") != _run_contract_owner_digest(owner):
        raise ValueError("run contract owner digest mismatch")
    return canonical_snapshot


def bind_run_contract_owner(
    config: Mapping[str, Any],
    *,
    activated_at: datetime,
    handoff_reason: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Bind or verify the one immutable owner for a bounded strategy run.

    A missing owner is written once before the first start.  Once present, a
    different canonical contract is rejected unless the caller supplies an
    explicit handoff reason; this prevents a control rewrite or service restart
    from silently changing the active run's target, window, terminal order cap,
    or exit terms.  Normal per-order sizing remains recovery-adjustable.
    """

    prepared = dict(config)
    candidate_snapshot = run_contract_snapshot_from_config(prepared)
    bounded = candidate_snapshot.get("run_end_time") is not None or candidate_snapshot.get(
        "max_cumulative_notional"
    ) is not None
    raw_owner = prepared.get(RUN_CONTRACT_OWNER_KEY)
    normalized_reason = str(handoff_reason or "").strip() or None

    materialized_cap_changed = False
    if bounded:
        raw_cap = _normalize_contract_number(
            prepared.get("terminal_drain_max_order_notional", 0.0),
            "terminal_drain_max_order_notional",
        )
        effective_cap = float(
            candidate_snapshot.get("terminal_drain_max_order_notional") or 0.0
        )
        if (raw_cap is None or raw_cap <= 0.0) and effective_cap > 0.0:
            prepared["terminal_drain_max_order_notional"] = effective_cap
            candidate_snapshot = run_contract_snapshot_from_config(prepared)
            materialized_cap_changed = True

    if not bounded:
        if raw_owner is not None:
            validate_run_contract_owner(raw_owner, expected_symbol=str(prepared.get("symbol") or ""))
            raise ValueError(
                "active bounded run contract cannot be removed without an explicit deactivation handoff"
            )
        return prepared, False

    if raw_owner is None:
        prepared[RUN_CONTRACT_OWNER_KEY] = _new_run_contract_owner(
            candidate_snapshot,
            activated_at=activated_at,
            generation=1,
            initial_per_order_notional=float(
                _normalize_contract_number(
                    prepared.get("per_order_notional", 0.0),
                    "per_order_notional",
                )
                or 0.0
            ),
            handoff_from_contract_id=None,
            handoff_reason=None,
        )
        return prepared, True

    validate_run_contract_owner(
        raw_owner,
        expected_symbol=str(candidate_snapshot.get("symbol") or ""),
    )
    candidate_id = run_contract_identity_from_config(candidate_snapshot)
    candidate_digest = run_contract_snapshot_digest(candidate_snapshot)
    if (
        raw_owner.get("run_contract_id") == candidate_id
        and raw_owner.get("run_contract_digest") == candidate_digest
    ):
        return prepared, materialized_cap_changed

    if normalized_reason is None:
        raise ValueError(
            "active run contract differs from control; explicit run contract handoff is required"
        )
    prepared[RUN_CONTRACT_OWNER_KEY] = _new_run_contract_owner(
        candidate_snapshot,
        activated_at=activated_at,
        generation=int(raw_owner["generation"]) + 1,
        initial_per_order_notional=float(
            _normalize_contract_number(
                prepared.get("per_order_notional", 0.0),
                "per_order_notional",
            )
            or 0.0
        ),
        handoff_from_contract_id=str(raw_owner["run_contract_id"]),
        handoff_reason=normalized_reason,
    )
    return prepared, True


def resolve_authoritative_run_contract(
    config: Mapping[str, Any],
    *,
    expected_symbol: str | None = None,
) -> tuple[dict[str, Any], str]:
    """Resolve the only contract consumers may use for a bounded run.

    Unbounded diagnostic runs have no durable owner.  A bounded run must carry
    a complete owner, and the current raw contract fields must still canonicalize
    to the owner's frozen snapshot.  Consumers therefore cannot silently accept
    a target/window/exit rewrite while retaining a stale owner.
    """

    raw_snapshot = run_contract_snapshot_from_config(config)
    bounded = raw_snapshot.get("run_end_time") is not None or raw_snapshot.get(
        "max_cumulative_notional"
    ) is not None
    raw_owner = config.get(RUN_CONTRACT_OWNER_KEY)
    normalized_expected = str(expected_symbol or "").upper().strip()

    if not bounded:
        if raw_owner is not None:
            validate_run_contract_owner(
                raw_owner,
                expected_symbol=normalized_expected or None,
            )
            raise ValueError(
                "active bounded run contract owner cannot authorize an unbounded config"
            )
        if normalized_expected and raw_snapshot.get("symbol") != normalized_expected:
            raise ValueError("run contract expected symbol mismatch")
        return raw_snapshot, run_contract_identity_from_config(raw_snapshot)

    if not isinstance(raw_owner, Mapping):
        raise ValueError("bounded run contract owner is missing")
    owner_snapshot = validate_run_contract_owner(
        raw_owner,
        expected_symbol=normalized_expected or None,
    )
    if raw_snapshot != owner_snapshot:
        raise ValueError("raw run contract does not match authoritative owner")
    owner_contract_id = str(raw_owner.get("run_contract_id") or "")
    if owner_contract_id != run_contract_identity_from_config(owner_snapshot):
        raise ValueError("authoritative run contract id mismatch")
    return owner_snapshot, owner_contract_id


@dataclass(frozen=True)
class RunLifecycleSnapshot:
    symbol: str
    captured_at: datetime
    target_value: float
    achieved_value: float
    deadline: datetime
    observed_rate: float | None
    recovery_intent: RecoveryIntent | None
    terminal_condition_reason: str | None
    terminal_drain: TerminalDrainFeedback | None
    stop_preserve_requested: bool = False
    stop_preserve_reason: str | None = None

    def __post_init__(self) -> None:
        if not str(self.symbol).strip():
            raise ValueError("symbol is required")
        if not math.isfinite(float(self.target_value)) or self.target_value <= 0:
            raise ValueError("target_value must be positive and finite")
        if not math.isfinite(float(self.achieved_value)) or self.achieved_value < 0:
            raise ValueError("achieved_value must be non-negative and finite")
        if self.observed_rate is not None and (
            not math.isfinite(float(self.observed_rate)) or self.observed_rate < 0
        ):
            raise ValueError("observed_rate must be non-negative and finite")
        if self.terminal_condition_reason is not None and not str(self.terminal_condition_reason).strip():
            raise ValueError("terminal_condition_reason cannot be blank")
        if self.stop_preserve_requested and not str(self.stop_preserve_reason or "").strip():
            raise ValueError("explicit stop-preserve requires a reason")
        if not self.stop_preserve_requested and self.stop_preserve_reason is not None:
            raise ValueError("stop_preserve_reason requires stop_preserve_requested")


@dataclass(frozen=True)
class RunState:
    symbol: str
    run_id: str
    generation: int
    phase: RunPhase
    outcome: RunOutcome | None
    target_value: float | None
    deadline: datetime | None
    terminal_achieved_value: float | None
    terminal_shortfall: float | None
    terminal_reason: str | None
    recovery_attempts: int
    exit_blocked_attempts: int
    inventory_preserved: bool
    last_action_id: LifecycleActionId | None

    @classmethod
    def initial(cls, symbol: str, *, run_id: str) -> "RunState":
        normalized = str(symbol).upper().strip()
        if not normalized:
            raise ValueError("symbol is required")
        if not str(run_id).strip():
            raise ValueError("run_id is required")
        return cls(
            symbol=normalized,
            run_id=str(run_id),
            generation=0,
            phase=RunPhase.RUNNING,
            outcome=None,
            target_value=None,
            deadline=None,
            terminal_achieved_value=None,
            terminal_shortfall=None,
            terminal_reason=None,
            recovery_attempts=0,
            exit_blocked_attempts=0,
            inventory_preserved=False,
            last_action_id=None,
        )


@dataclass(frozen=True)
class RunLifecycleDecision:
    """Exactly one top-level action for a symbol in one lifecycle cycle."""

    action_id: LifecycleActionId
    next_state: RunState
    intent: LifecycleIntent | None = None
    reasons: tuple[str, ...] = ()
    stop_allowed: bool = False

    def __post_init__(self) -> None:
        stop_actions = {LifecycleActionId.STOP_CLEAN, LifecycleActionId.STOP_PRESERVE}
        if self.stop_allowed != (self.action_id in stop_actions):
            raise ValueError("stop_allowed must match a stop action")


def _advance(
    state: RunState,
    action_id: LifecycleActionId,
    **changes: object,
) -> RunState:
    return replace(
        state,
        generation=state.generation + 1,
        last_action_id=action_id,
        **changes,
    )


def _bind_run_contract(state: RunState, snapshot: RunLifecycleSnapshot) -> RunState:
    if state.target_value is None:
        return replace(state, target_value=float(snapshot.target_value), deadline=snapshot.deadline)
    if not math.isclose(state.target_value, float(snapshot.target_value), rel_tol=0.0, abs_tol=1e-12):
        raise ValueError("target_value cannot change during a run")
    if state.deadline != snapshot.deadline:
        raise ValueError("deadline cannot change during a run")
    return state


def _start_terminal_drain(
    *,
    state: RunState,
    snapshot: RunLifecycleSnapshot,
    outcome: RunOutcome,
    reason: str,
) -> RunLifecycleDecision:
    target = float(state.target_value if state.target_value is not None else snapshot.target_value)
    achieved = float(snapshot.achieved_value)
    shortfall = max(target - achieved, 0.0)
    action_id = LifecycleActionId.START_TERMINAL_DRAIN
    return RunLifecycleDecision(
        action_id=action_id,
        next_state=_advance(
            state,
            action_id,
            phase=RunPhase.TERMINAL_DRAIN,
            outcome=outcome,
            terminal_achieved_value=achieved,
            terminal_shortfall=shortfall,
            terminal_reason=reason,
            inventory_preserved=False,
        ),
        intent=LifecycleIntent(action_id="start_terminal_drain", reason=reason),
        reasons=(reason,),
    )


def _stop_preserved(state: RunState, snapshot: RunLifecycleSnapshot) -> RunLifecycleDecision:
    reason = str(snapshot.stop_preserve_reason)
    target = float(state.target_value if state.target_value is not None else snapshot.target_value)
    achieved = float(snapshot.achieved_value)
    action_id = LifecycleActionId.STOP_PRESERVE
    return RunLifecycleDecision(
        action_id=action_id,
        next_state=_advance(
            state,
            action_id,
            phase=RunPhase.STOPPED_PRESERVED,
            outcome=RunOutcome.STOP_PRESERVE,
            terminal_achieved_value=achieved,
            terminal_shortfall=max(target - achieved, 0.0),
            terminal_reason=reason,
            inventory_preserved=True,
        ),
        reasons=(reason,),
        stop_allowed=True,
    )


def _blocked_decision(
    *,
    state: RunState,
    reasons: tuple[str, ...],
) -> RunLifecycleDecision:
    first_block = state.phase != RunPhase.EXIT_BLOCKED
    action_id = (
        LifecycleActionId.MARK_EXIT_BLOCKED
        if first_block
        else LifecycleActionId.RETRY_TERMINAL_DRAIN
    )
    return RunLifecycleDecision(
        action_id=action_id,
        next_state=_advance(
            state,
            action_id,
            phase=RunPhase.EXIT_BLOCKED,
            exit_blocked_attempts=state.exit_blocked_attempts + 1,
        ),
        intent=(
            None
            if first_block
            else LifecycleIntent(action_id="retry_terminal_drain", reason=reasons[0])
        ),
        reasons=reasons,
    )


def _decide_terminal_phase(
    *,
    state: RunState,
    snapshot: RunLifecycleSnapshot,
    policy: RunLifecyclePolicy,
) -> RunLifecycleDecision:
    feedback = snapshot.terminal_drain
    if feedback is None:
        if state.phase == RunPhase.EXIT_BLOCKED:
            return _blocked_decision(state=state, reasons=("terminal_drain_feedback_missing",))
        action_id = LifecycleActionId.RUN_TERMINAL_DRAIN
        return RunLifecycleDecision(
            action_id=action_id,
            next_state=_advance(state, action_id, phase=RunPhase.TERMINAL_DRAIN),
            intent=LifecycleIntent(
                action_id="decide_terminal_drain",
                reason="terminal_drain_feedback_missing",
            ),
            reasons=("terminal_drain_feedback_missing",),
        )

    if feedback.stop_allowed:
        if feedback.flat_proof is None:
            return _blocked_decision(state=state, reasons=("flat_proof_missing",))
        proof_errors = feedback.flat_proof.validation_errors(
            observed_at=snapshot.captured_at,
            policy=policy,
        )
        if proof_errors:
            return _blocked_decision(state=state, reasons=proof_errors)
        action_id = LifecycleActionId.STOP_CLEAN
        return RunLifecycleDecision(
            action_id=action_id,
            next_state=_advance(
                state,
                action_id,
                phase=RunPhase.STOPPED_CLEAN,
                inventory_preserved=False,
            ),
            reasons=("clean_flat_proof",),
            stop_allowed=True,
        )

    if feedback.blocked_reason is not None:
        return _blocked_decision(state=state, reasons=(str(feedback.blocked_reason),))

    if feedback.action_id is not None:
        retrying = state.phase == RunPhase.EXIT_BLOCKED
        action_id = (
            LifecycleActionId.RETRY_TERMINAL_DRAIN
            if retrying
            else LifecycleActionId.RUN_TERMINAL_DRAIN
        )
        return RunLifecycleDecision(
            action_id=action_id,
            next_state=_advance(state, action_id, phase=RunPhase.TERMINAL_DRAIN),
            intent=LifecycleIntent(
                action_id=str(feedback.action_id),
                reason="terminal_drain_action",
            ),
            reasons=("terminal_drain_action",),
        )

    return _blocked_decision(state=state, reasons=("terminal_drain_no_action",))


def decide_run_lifecycle(
    *,
    state: RunState,
    snapshot: RunLifecycleSnapshot,
    policy: RunLifecyclePolicy,
) -> RunLifecycleDecision:
    """Select one lifecycle action for one symbol and one evaluation cycle."""

    normalized_symbol = str(snapshot.symbol).upper().strip()
    if normalized_symbol != state.symbol:
        raise ValueError("snapshot symbol does not match lifecycle owner")
    state = _bind_run_contract(state, snapshot)

    if state.phase == RunPhase.STOPPED_CLEAN:
        return RunLifecycleDecision(
            action_id=LifecycleActionId.STOP_CLEAN,
            next_state=state,
            reasons=("already_stopped_clean",),
            stop_allowed=True,
        )
    if state.phase == RunPhase.STOPPED_PRESERVED:
        return RunLifecycleDecision(
            action_id=LifecycleActionId.STOP_PRESERVE,
            next_state=state,
            reasons=("already_stopped_preserved",),
            stop_allowed=True,
        )

    if snapshot.stop_preserve_requested:
        return _stop_preserved(state, snapshot)

    if state.phase in {RunPhase.TERMINAL_DRAIN, RunPhase.EXIT_BLOCKED}:
        return _decide_terminal_phase(state=state, snapshot=snapshot, policy=policy)

    target = float(state.target_value if state.target_value is not None else snapshot.target_value)
    achieved = float(snapshot.achieved_value)
    if achieved >= target:
        return _start_terminal_drain(
            state=state,
            snapshot=snapshot,
            outcome=RunOutcome.TARGET_REACHED,
            reason="target_reached",
        )

    deadline = state.deadline if state.deadline is not None else snapshot.deadline
    if snapshot.captured_at >= deadline:
        return _start_terminal_drain(
            state=state,
            snapshot=snapshot,
            outcome=RunOutcome.TARGET_UNMET_DEADLINE,
            reason="deadline_reached",
        )

    if snapshot.terminal_condition_reason is not None:
        return _start_terminal_drain(
            state=state,
            snapshot=snapshot,
            outcome=RunOutcome.CONDITION_UNMET,
            reason=str(snapshot.terminal_condition_reason),
        )

    remaining_seconds = (deadline - snapshot.captured_at).total_seconds()
    required_rate = max(target - achieved, 0.0) / remaining_seconds
    recovery = snapshot.recovery_intent
    if recovery is None and snapshot.observed_rate is not None and snapshot.observed_rate < required_rate:
        recovery = RecoveryIntent(
            action_id="restore_required_pace",
            reason="pace_below_required",
        )

    if recovery is not None:
        action_id = LifecycleActionId.APPLY_RECOVERY
        return RunLifecycleDecision(
            action_id=action_id,
            next_state=_advance(
                state,
                action_id,
                phase=RunPhase.RECOVERING,
                recovery_attempts=state.recovery_attempts + 1,
            ),
            intent=recovery,
            reasons=(recovery.reason,),
        )

    action_id = LifecycleActionId.CONTINUE_TARGET
    return RunLifecycleDecision(
        action_id=action_id,
        next_state=_advance(state, action_id, phase=RunPhase.RUNNING),
        reasons=("target_in_progress",),
    )
