"""Restart-safe JSON persistence for the futures recovery coordinator.

This adapter owns no recovery decisions and executes no effects.  It persists
one symbol's complete immutable state together with its desired managed control
profile under the project's existing file lock and atomic JSON replacement.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping as MappingABC
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType, UnionType
from typing import Any, Mapping, Union, get_args, get_origin, get_type_hints

from .futures_recovery_coordinator import (
    ActionId,
    EffectStage,
    ExecutionPolicy,
    ManagedProfile,
    OrderRole,
    RecoveryPhase,
    RecoveryState,
)
from .recovery_control_ownership import (
    exclusive_control_lock,
    write_control_json_atomically,
)


RECOVERY_STATE_KEY = "_futures_recovery_state"
RECOVERY_STATE_MIRROR_KEY = "_futures_recovery_state_mirror"
RECOVERY_STATE_SCHEMA_VERSION = 1
_RECOVERY_STATE_SLOT_KEYS = (RECOVERY_STATE_KEY, RECOVERY_STATE_MIRROR_KEY)


def recovery_coordinator_registered(control: Mapping[str, Any]) -> bool:
    """Return whether the control document has crossed the coordinator fence.

    Presence is the ownership boundary.  A malformed envelope must not silently
    hand actuation back to a legacy writer; the coordinator/store path owns
    validation and repair after registration.
    """

    return any(key in control for key in _RECOVERY_STATE_SLOT_KEYS)


_PROTECTED_MANAGED_CONTROL_KEYS = frozenset(
    {
        "best_quote_maker_volume_allow_loss_reduce_only",
        "best_quote_maker_volume_net_loss_reduce_enabled",
        "hard_loss_forced_reduce_enabled",
        "volatility_entry_pause_enabled",
    }
)
_COORDINATOR_OVERLAY_CONTROL_KEYS = frozenset(
    {
        "recovery_action_lease_active",
        "recovery_action_lease_epoch",
        "recovery_action_lease_hard_expires_at",
        "recovery_action_lease_order_role",
        "recovery_action_lease_side",
        "recovery_entry_blocked_sides",
        "recovery_order_ledger_class",
        "recovery_order_role",
        "recovery_order_side",
        "recovery_reduce_only_allowed_sides",
        "recovery_safety_evidence_fingerprint",
        "recovery_safety_lease_epoch",
        "recovery_safety_lease_hard_expires_at",
        "recovery_safety_observation_seq",
        "recovery_safety_observed_at",
        "best_quote_maker_volume_same_side_entry_price_guard_report_only",
        "near_market_entry_max_center_distance_steps",
        "near_market_reentry_confirm_cycles",
    }
)


def _is_managed_recovery_key(key: str) -> bool:
    return (
        key in _COORDINATOR_OVERLAY_CONTROL_KEYS
        or key in _PROTECTED_MANAGED_CONTROL_KEYS
    )


class RecoveryStateStoreError(RuntimeError):
    """Base error for durable recovery-state access."""


class RecoveryStateUnavailableError(RecoveryStateStoreError):
    """The control document has no registered recovery state."""


class RecoveryStateCorruptError(RecoveryStateStoreError):
    """The control document or embedded recovery state is unsafe to use."""


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol).upper().strip()
    if not normalized:
        raise ValueError("symbol is required")
    return normalized


def _require_fields(
    payload: Mapping[str, Any], expected: set[str], *, path: str
) -> None:
    missing = sorted(expected.difference(payload))
    if missing:
        raise RecoveryStateCorruptError(f"{path} missing fields: {', '.join(missing)}")


def _json_copy(value: Any, *, path: str) -> Any:
    """Detach a value into strict JSON data without accepting NaN/Infinity."""

    if isinstance(value, MappingABC):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise RecoveryStateCorruptError(
                    f"{path} contains a non-string JSON object key"
                )
            result[key] = _json_copy(item, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _json_copy(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise RecoveryStateCorruptError(
        f"{path} contains unsupported JSON value {type(value).__name__}"
    )


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _encode_datetime(value: datetime, *, path: str) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RecoveryStateCorruptError(
            f"{path} must be an absolute timezone-aware datetime"
        )
    return value.isoformat()


def _decode_datetime(value: Any, *, path: str) -> datetime:
    if not isinstance(value, str):
        raise RecoveryStateCorruptError(f"{path} must be an ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RecoveryStateCorruptError(f"{path} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecoveryStateCorruptError(
            f"{path} must be an absolute timezone-aware datetime"
        )
    return parsed


def _is_union(annotation: Any) -> bool:
    return get_origin(annotation) in (Union, UnionType)


@lru_cache(maxsize=None)
def _resolved_hints(dataclass_type: type[Any]) -> dict[str, Any]:
    return get_type_hints(dataclass_type)


def _encode_typed(value: Any, annotation: Any, *, path: str) -> Any:
    if annotation is Any:
        return _json_copy(value, path=path)

    if _is_union(annotation):
        variants = get_args(annotation)
        if value is None and type(None) in variants:
            return None
        candidates = tuple(item for item in variants if item is not type(None))
        if len(candidates) != 1:
            raise RecoveryStateCorruptError(f"unsupported union type at {path}")
        return _encode_typed(value, candidates[0], path=path)

    if annotation is datetime:
        if not isinstance(value, datetime):
            raise RecoveryStateCorruptError(f"{path} must be a datetime")
        return _encode_datetime(value, path=path)

    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if not isinstance(value, annotation):
            raise RecoveryStateCorruptError(f"{path} must be {annotation.__name__}")
        return value.value

    origin = get_origin(annotation)
    if origin is tuple:
        if not isinstance(value, tuple):
            raise RecoveryStateCorruptError(f"{path} must be a tuple")
        item_types = get_args(annotation)
        if len(item_types) == 2 and item_types[1] is Ellipsis:
            return [
                _encode_typed(item, item_types[0], path=f"{path}[]") for item in value
            ]
        if len(item_types) != len(value):
            raise RecoveryStateCorruptError(f"{path} tuple length is invalid")
        return [
            _encode_typed(item, item_type, path=f"{path}[{index}]")
            for index, (item, item_type) in enumerate(zip(value, item_types))
        ]

    if origin is MappingABC:
        if not isinstance(value, MappingABC):
            raise RecoveryStateCorruptError(f"{path} must be a mapping")
        key_type, value_type = get_args(annotation)
        if key_type is str:
            result: dict[str, Any] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise RecoveryStateCorruptError(f"{path} must have string keys")
                result[key] = _encode_typed(item, value_type, path=f"{path}.{key}")
            return result
        rows = [
            {
                "key": _encode_typed(key, key_type, path=f"{path}[].key"),
                "value": _encode_typed(item, value_type, path=f"{path}[].value"),
            }
            for key, item in value.items()
        ]
        return sorted(
            rows,
            key=lambda row: json.dumps(
                row["key"], sort_keys=True, separators=(",", ":")
            ),
        )

    if isinstance(annotation, type) and is_dataclass(annotation):
        if not isinstance(value, annotation):
            raise RecoveryStateCorruptError(f"{path} must be {annotation.__name__}")
        hints = _resolved_hints(annotation)
        return {
            item.name: _encode_typed(
                getattr(value, item.name),
                hints[item.name],
                path=f"{path}.{item.name}",
            )
            for item in fields(annotation)
        }

    if annotation is bool:
        if type(value) is not bool:
            raise RecoveryStateCorruptError(f"{path} must be a boolean")
        return value
    if annotation is int:
        if type(value) is not int:
            raise RecoveryStateCorruptError(f"{path} must be an integer")
        return value
    if annotation is float:
        if type(value) not in (int, float) or not math.isfinite(float(value)):
            raise RecoveryStateCorruptError(f"{path} must be a finite number")
        return value
    if annotation is str:
        if not isinstance(value, str):
            raise RecoveryStateCorruptError(f"{path} must be a string")
        return value
    raise RecoveryStateCorruptError(f"unsupported state type at {path}: {annotation}")


def _decode_typed(value: Any, annotation: Any, *, path: str) -> Any:
    if annotation is Any:
        return _freeze_json(_json_copy(value, path=path))

    if _is_union(annotation):
        variants = get_args(annotation)
        if value is None:
            if type(None) not in variants:
                raise RecoveryStateCorruptError(f"{path} cannot be null")
            return None
        candidates = tuple(item for item in variants if item is not type(None))
        if len(candidates) != 1:
            raise RecoveryStateCorruptError(f"unsupported union type at {path}")
        return _decode_typed(value, candidates[0], path=path)

    if annotation is datetime:
        return _decode_datetime(value, path=path)

    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if not isinstance(value, str):
            raise RecoveryStateCorruptError(f"{path} must be an enum string")
        try:
            return annotation(value)
        except ValueError as exc:
            raise RecoveryStateCorruptError(
                f"{path} has unknown value {value!r}"
            ) from exc

    origin = get_origin(annotation)
    if origin is tuple:
        if not isinstance(value, list):
            raise RecoveryStateCorruptError(f"{path} must be a list")
        item_types = get_args(annotation)
        if len(item_types) == 2 and item_types[1] is Ellipsis:
            return tuple(
                _decode_typed(item, item_types[0], path=f"{path}[]") for item in value
            )
        if len(item_types) != len(value):
            raise RecoveryStateCorruptError(f"{path} list length is invalid")
        return tuple(
            _decode_typed(item, item_type, path=f"{path}[{index}]")
            for index, (item, item_type) in enumerate(zip(value, item_types))
        )

    if origin is MappingABC:
        key_type, value_type = get_args(annotation)
        result: dict[Any, Any] = {}
        if key_type is str:
            if not isinstance(value, MappingABC):
                raise RecoveryStateCorruptError(f"{path} must be an object")
            for key, item in value.items():
                if not isinstance(key, str):
                    raise RecoveryStateCorruptError(f"{path} must have string keys")
                result[key] = _decode_typed(item, value_type, path=f"{path}.{key}")
        else:
            if not isinstance(value, list):
                raise RecoveryStateCorruptError(f"{path} must be a list")
            for row in value:
                if not isinstance(row, MappingABC):
                    raise RecoveryStateCorruptError(f"{path}[] must be an object")
                _require_fields(row, {"key", "value"}, path=f"{path}[]")
                key = _decode_typed(row["key"], key_type, path=f"{path}[].key")
                if key in result:
                    raise RecoveryStateCorruptError(f"{path} contains a duplicate key")
                result[key] = _decode_typed(
                    row["value"], value_type, path=f"{path}[].value"
                )
        return MappingProxyType(result)

    if isinstance(annotation, type) and is_dataclass(annotation):
        if not isinstance(value, MappingABC):
            raise RecoveryStateCorruptError(f"{path} must be an object")
        dataclass_fields = fields(annotation)
        _require_fields(value, {item.name for item in dataclass_fields}, path=path)
        hints = _resolved_hints(annotation)
        instance = annotation(
            **{
                item.name: _decode_typed(
                    value[item.name],
                    hints[item.name],
                    path=f"{path}.{item.name}",
                )
                for item in dataclass_fields
            }
        )
        if isinstance(instance, ManagedProfile):
            _validate_profile(instance, path=path)
        return instance

    if annotation is bool:
        if type(value) is not bool:
            raise RecoveryStateCorruptError(f"{path} must be a boolean")
        return value
    if annotation is int:
        if type(value) is not int:
            raise RecoveryStateCorruptError(f"{path} must be an integer")
        return value
    if annotation is float:
        if type(value) not in (int, float) or not math.isfinite(float(value)):
            raise RecoveryStateCorruptError(f"{path} must be a finite number")
        return float(value)
    if annotation is str:
        if not isinstance(value, str):
            raise RecoveryStateCorruptError(f"{path} must be a string")
        return value
    raise RecoveryStateCorruptError(f"unsupported state type at {path}: {annotation}")


def _profile_digest(profile: ManagedProfile) -> str:
    payload = _json_copy(profile.fields, path="profile.fields")
    payload["_recovery_execution_policy"] = {
        "order_type": profile.execution_policy.order_type,
        "time_in_force": profile.execution_policy.time_in_force,
        "post_only": profile.execution_policy.post_only,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_profile(profile: ManagedProfile, *, path: str) -> None:
    if any(key in profile.fields for key in _RECOVERY_STATE_SLOT_KEYS):
        raise RecoveryStateCorruptError(
            f"{path}.fields cannot own the recovery state namespace"
        )
    if profile.execution_policy != ExecutionPolicy():
        raise RecoveryStateCorruptError(
            f"{path}.execution_policy must remain LIMIT/GTX/post-only"
        )
    if profile.digest != _profile_digest(profile):
        raise RecoveryStateCorruptError(f"{path}.digest does not match fields")


def _validate_state(state: RecoveryState) -> None:
    if state.symbol != _normalize_symbol(state.symbol):
        raise RecoveryStateCorruptError("state.symbol must be normalized")
    for name in (
        "document_revision",
        "generation",
        "action_lease_epoch",
        "safety_lease_epoch",
        "effect_epoch",
    ):
        value = getattr(state, name)
        if type(value) is not int or value < 0:
            raise RecoveryStateCorruptError(f"state.{name} must be non-negative")
    if state.pending_effect_stage is EffectStage.NONE:
        if state.pending_effect_epoch is not None:
            raise RecoveryStateCorruptError(
                "state.pending_effect_epoch requires a pending effect stage"
            )
    elif (
        state.pending_effect_epoch is None
        or state.pending_effect_epoch <= 0
        or state.pending_effect_epoch != state.effect_epoch
    ):
        raise RecoveryStateCorruptError(
            "state pending effect stage and epoch must identify the same effect"
        )
    if state.action_lease is not None:
        lease = state.action_lease
        if (
            lease.epoch < 0
            or lease.epoch != state.action_lease_epoch
            or lease.action_id is not ActionId.TEMPORARY_LOSS_RELIEF
            or lease.order_role is not OrderRole.REDUCE_ONLY
        ):
            raise RecoveryStateCorruptError(
                "state.action_lease must be the current typed loss lease"
            )
        if lease.hard_expires_at < lease.started_at:
            raise RecoveryStateCorruptError(
                "state.action_lease hard expiry precedes its start"
            )
        if lease.activated_at is not None and not (
            lease.started_at <= lease.activated_at <= lease.hard_expires_at
        ):
            raise RecoveryStateCorruptError(
                "state.action_lease activation lies outside its absolute lease"
            )
    if state.safety_lease is not None:
        lease = state.safety_lease
        if lease.epoch < 0 or lease.epoch != state.safety_lease_epoch:
            raise RecoveryStateCorruptError(
                "state.safety_lease must use the current safety lease epoch"
            )
        if lease.observation_seq < 0:
            raise RecoveryStateCorruptError(
                "state.safety_lease.observation_seq must be non-negative"
            )
        if lease.hard_expires_at < lease.started_at:
            raise RecoveryStateCorruptError(
                "state.safety_lease hard expiry precedes its start"
            )
    if state.cleanup_obligation is not None:
        cleanup = state.cleanup_obligation
        manifest = cleanup.managed_order_manifest
        if (
            manifest.symbol != state.symbol
            or manifest.generation > state.generation
            or manifest.issued_at > cleanup.created_at
            or cleanup.next_retry_at < cleanup.created_at
            or cleanup.needs_manifest_rebuild != (not manifest.orders)
        ):
            raise RecoveryStateCorruptError(
                "state.cleanup_obligation has an invalid managed manifest"
            )
        if cleanup.attempt_count < 0:
            raise RecoveryStateCorruptError(
                "state.cleanup_obligation.attempt_count must be non-negative"
            )
    for episode, count in state.temporary_loss_lease_uses.items():
        if not isinstance(episode, str) or not episode:
            raise RecoveryStateCorruptError(
                "state.temporary_loss_lease_uses needs non-empty episode keys"
            )
        if type(count) is not int or count < 0:
            raise RecoveryStateCorruptError(
                f"state.temporary_loss_lease_uses.{episode} must be non-negative"
            )
    _validate_profile(state.baseline_profile, path="state.baseline_profile")
    _validate_profile(state.desired_profile, path="state.desired_profile")

    baseline = state.baseline_profile.fields
    desired = state.desired_profile.fields
    for name in (
        "best_quote_maker_volume_allow_loss_reduce_only",
        "best_quote_maker_volume_net_loss_reduce_enabled",
        "hard_loss_forced_reduce_enabled",
    ):
        if baseline.get(name) is not False:
            raise RecoveryStateCorruptError(
                f"state.baseline_profile.{name} must be false"
            )
    if baseline.get("volatility_entry_pause_enabled") is not True:
        raise RecoveryStateCorruptError(
            "state.baseline_profile.volatility_entry_pause_enabled must be true"
        )
    for name in (
        "best_quote_maker_volume_net_loss_reduce_enabled",
        "hard_loss_forced_reduce_enabled",
    ):
        if desired.get(name) is not False:
            raise RecoveryStateCorruptError(
                f"state.desired_profile.{name} must be false"
            )
    if desired.get("volatility_entry_pause_enabled") is not True:
        raise RecoveryStateCorruptError(
            "state.desired_profile.volatility_entry_pause_enabled must be true"
        )

    allow_loss = desired.get("best_quote_maker_volume_allow_loss_reduce_only")
    if type(allow_loss) is not bool:
        raise RecoveryStateCorruptError(
            "state.desired_profile allow_loss field must be boolean"
        )
    phase = state.phase
    action_lease = state.action_lease
    safety_lease = state.safety_lease
    cleanup = state.cleanup_obligation

    cleaning_needs_action_lease = bool(
        phase is RecoveryPhase.CLEANING
        and cleanup is not None
        and (
            cleanup.source_action_id is ActionId.TEMPORARY_LOSS_RELIEF
            or state.cleanup_successor_action is ActionId.TEMPORARY_LOSS_RELIEF
        )
    )
    action_lease_allowed = cleaning_needs_action_lease or (
        phase in {RecoveryPhase.SETTLING, RecoveryPhase.ACTIVE}
        and state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
    )
    if (action_lease is not None) != action_lease_allowed:
        raise RecoveryStateCorruptError(
            f"state.{phase.value} has a forbidden action lease combination"
        )
    if (
        (
            phase in {RecoveryPhase.SETTLING, RecoveryPhase.ACTIVE}
            or (
                phase is RecoveryPhase.CLEANING
                and state.cleanup_successor_action
                is ActionId.TEMPORARY_LOSS_RELIEF
            )
        )
        and state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
        and action_lease is not None
        and state.hard_expires_at != action_lease.hard_expires_at
    ):
        raise RecoveryStateCorruptError(
            "state.hard_expires_at must match state.action_lease.hard_expires_at"
        )
    if (
        (
            phase in {RecoveryPhase.SETTLING, RecoveryPhase.ACTIVE}
            or (
                phase is RecoveryPhase.CLEANING
                and state.cleanup_successor_action
                is ActionId.TEMPORARY_LOSS_RELIEF
            )
        )
        and state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
        and action_lease is not None
        and state.issued_at != action_lease.started_at
    ):
        raise RecoveryStateCorruptError(
            "state.issued_at must match state.action_lease.started_at"
        )

    safety_lease_profile_bound = bool(
        state.active_action is ActionId.SAFETY_CONVERGE
        and (
            phase is RecoveryPhase.ACTIVE
            or (
                phase is RecoveryPhase.CLEANING
                and state.cleanup_successor_action is ActionId.SAFETY_CONVERGE
            )
        )
    )
    safety_lease_historical = bool(
        state.active_action is ActionId.SAFETY_CONVERGE
        and phase in {RecoveryPhase.RESTORING, RecoveryPhase.COOLDOWN}
    )
    if (safety_lease is not None) != (
        safety_lease_profile_bound or safety_lease_historical
    ):
        raise RecoveryStateCorruptError(
            f"state.{phase.value} has a forbidden safety lease combination"
        )
    if (
        safety_lease_profile_bound
        and safety_lease is not None
        and not (
            desired.get("recovery_safety_lease_epoch") == safety_lease.epoch
            and desired.get("recovery_safety_lease_hard_expires_at")
            == safety_lease.hard_expires_at.isoformat()
            and desired.get("recovery_safety_evidence_fingerprint")
            == safety_lease.evidence_fingerprint
            and desired.get("recovery_safety_observation_seq")
            == safety_lease.observation_seq
            and desired.get("recovery_safety_observed_at")
            == safety_lease.observed_at.isoformat()
        )
    ):
        raise RecoveryStateCorruptError(
            "state safety lease does not match desired profile metadata"
        )
    if (
        safety_lease_profile_bound
        and safety_lease is not None
        and state.hard_expires_at != safety_lease.hard_expires_at
    ):
        raise RecoveryStateCorruptError(
            "state.hard_expires_at must match state.safety_lease.hard_expires_at"
        )
    if safety_lease_historical and any(
        str(key).startswith("recovery_safety_") for key in desired
    ):
        raise RecoveryStateCorruptError(
            "historical safety lease cannot remain active in desired profile"
        )

    if (cleanup is not None) != (phase is RecoveryPhase.CLEANING):
        raise RecoveryStateCorruptError(
            f"state.{phase.value} has a forbidden cleanup combination"
        )
    if cleanup is not None:
        manifest = cleanup.managed_order_manifest
        source_loss_matches = bool(
            cleanup.source_action_id is ActionId.TEMPORARY_LOSS_RELIEF
            and action_lease is not None
            and manifest.action_lease_epoch == action_lease.epoch
            and manifest.side is action_lease.side
            and manifest.order_role is action_lease.order_role
            and manifest.hard_expires_at == action_lease.hard_expires_at
        )
        successor_loss_matches = bool(
            state.cleanup_successor_action is ActionId.TEMPORARY_LOSS_RELIEF
            and action_lease is not None
            and state.active_action is action_lease.action_id
            and state.side is action_lease.side
            and state.order_role is action_lease.order_role
        )
        if cleanup.source_action_id is ActionId.TEMPORARY_LOSS_RELIEF:
            lease_valid = source_loss_matches
        elif state.cleanup_successor_action is ActionId.TEMPORARY_LOSS_RELIEF:
            lease_valid = successor_loss_matches
        else:
            lease_valid = action_lease is None
        if not lease_valid:
            raise RecoveryStateCorruptError(
                "state cleaning obligation has an invalid loss lease binding"
            )
        successor = state.cleanup_successor_action
        expected_action = cleanup.source_action_id if successor is None else successor
        if state.active_action is not expected_action:
            raise RecoveryStateCorruptError(
                "state cleaning action does not match its cleanup successor"
            )
    elif (
        state.cleanup_successor_action is not None
        or state.post_cleanup_effect_stage is not EffectStage.NONE
    ):
        raise RecoveryStateCorruptError(
            f"state.{phase.value} cannot retain cleanup successor controls"
        )

    terminal_action = state.active_action is ActionId.TERMINAL_STOP
    terminal_phase = phase in {
        RecoveryPhase.CLEANING,
        RecoveryPhase.STOP_PENDING,
        RecoveryPhase.STOPPED,
    }
    if terminal_action != (
        terminal_phase
        and (
            phase is not RecoveryPhase.CLEANING
            or state.cleanup_successor_action is ActionId.TERMINAL_STOP
        )
    ):
        raise RecoveryStateCorruptError(
            f"state.{phase.value} has a forbidden terminal action combination"
        )
    expected_runner_state = "stopped" if terminal_action else "running"
    if state.desired_runner_state != expected_runner_state:
        raise RecoveryStateCorruptError(
            f"state.{phase.value} requires runner {expected_runner_state}"
        )
    if state.terminal_stop_confirmed and not (
        terminal_action and phase in {RecoveryPhase.CLEANING, RecoveryPhase.STOPPED}
    ):
        raise RecoveryStateCorruptError(
            f"state.{phase.value} cannot retain terminal confirmation"
        )

    if (
        phase in {RecoveryPhase.ACTIVE, RecoveryPhase.SETTLING}
        and state.active_action is not ActionId.SAFETY_CONVERGE
    ):
        if state.progress_deadline_at is None:
            raise RecoveryStateCorruptError(
                f"state.{phase.value} requires progress_deadline_at"
            )
        if state.hard_expires_at is None:
            raise RecoveryStateCorruptError(
                f"state.{phase.value} requires hard_expires_at"
            )

    if phase is RecoveryPhase.STABLE:
        if not (
            state.active_action is ActionId.NOOP
            and state.decision_id is None
            and state.side is None
            and state.order_role is None
            and state.issued_at is None
            and state.progress_deadline_at is None
            and state.hard_expires_at is None
            and state.cooldown_until is None
            and state.pending_effect_stage is EffectStage.NONE
            and state.desired_profile.digest == state.baseline_profile.digest
        ):
            raise RecoveryStateCorruptError(
                "state.stable has retained recovery controls"
            )
    elif phase is RecoveryPhase.SETTLING:
        lease = action_lease
        if lease is None or not (
            state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
            and lease.activated_at is None
            and state.side is lease.side
            and state.order_role is lease.order_role
            and allow_loss is False
            and desired.get("recovery_order_side") == lease.side.value
            and desired.get("recovery_order_role") == lease.order_role.value
            and desired.get("recovery_action_lease_active") is False
            and desired.get("recovery_action_lease_epoch") == lease.epoch
            and desired.get("recovery_action_lease_side") == lease.side.value
            and desired.get("recovery_action_lease_order_role")
            == lease.order_role.value
            and desired.get("recovery_action_lease_hard_expires_at")
            == lease.hard_expires_at.isoformat()
            and state.pending_effect_stage
            in {EffectStage.NONE, EffectStage.RUNNER_RESTART}
        ):
            raise RecoveryStateCorruptError(
                "state.settling temporary loss lease metadata is inconsistent"
            )
    elif phase is RecoveryPhase.ACTIVE:
        if (
            cleanup is not None
            or state.cooldown_until is not None
            or state.terminal_stop_confirmed
        ):
            raise RecoveryStateCorruptError("state.active has forbidden ownership")
        if state.active_action is ActionId.TEMPORARY_LOSS_RELIEF:
            lease = action_lease
            if lease is None or not (
                lease.activated_at is not None
                and state.side is lease.side
                and state.order_role is lease.order_role
                and allow_loss is True
                and desired.get("recovery_order_side") == lease.side.value
                and desired.get("recovery_order_role") == lease.order_role.value
                and desired.get("recovery_action_lease_active") is True
                and desired.get("recovery_action_lease_epoch") == lease.epoch
                and desired.get("recovery_action_lease_side") == lease.side.value
                and desired.get("recovery_action_lease_order_role")
                == lease.order_role.value
                and desired.get("recovery_action_lease_hard_expires_at")
                == lease.hard_expires_at.isoformat()
                and state.pending_effect_stage
                in {EffectStage.NONE, EffectStage.RUNNER_RESTART}
            ):
                raise RecoveryStateCorruptError(
                    "active temporary loss lease must match action, side, role, "
                    "activation, hard expiry, and desired profile metadata"
                )
        elif allow_loss:
            raise RecoveryStateCorruptError(
                "allow_loss=true requires one active bounded reduce-only lease"
            )
    elif phase is RecoveryPhase.CLEANING:
        if allow_loss:
            raise RecoveryStateCorruptError("state.cleaning must close allow_loss")
    elif phase is RecoveryPhase.RESTORING:
        if not (
            state.progress_deadline_at is not None
            and state.cooldown_until is None
            and state.pending_effect_stage
            in {EffectStage.NONE, EffectStage.RUNNER_RESTART}
            and state.desired_profile.digest == state.baseline_profile.digest
        ):
            raise RecoveryStateCorruptError(
                "state.restoring requires baseline profile and progress_deadline_at"
            )
    elif phase is RecoveryPhase.COOLDOWN:
        if not (
            state.cooldown_until is not None
            and state.progress_deadline_at is None
            and state.pending_effect_stage is EffectStage.NONE
            and state.desired_profile.digest == state.baseline_profile.digest
        ):
            raise RecoveryStateCorruptError(
                "state.cooldown requires cooldown_until and baseline profile"
            )
    elif phase is RecoveryPhase.STOP_PENDING:
        if not (
            not state.terminal_stop_confirmed
            and state.pending_effect_stage is EffectStage.RUNNER_STOP
            and cleanup is None
        ):
            raise RecoveryStateCorruptError(
                "state.STOP_PENDING requires one unconfirmed runner stop"
            )
    elif phase is RecoveryPhase.STOPPED:
        if not (
            state.terminal_stop_confirmed
            and state.pending_effect_stage is EffectStage.NONE
            and cleanup is None
        ):
            raise RecoveryStateCorruptError(
                "state.STOPPED requires confirmed terminal action and stopped runner"
            )

    if allow_loss and not (
        phase is RecoveryPhase.ACTIVE
        and state.active_action is ActionId.TEMPORARY_LOSS_RELIEF
    ):
        raise RecoveryStateCorruptError(
            "allow_loss=true requires one active bounded reduce-only lease"
        )


def _encode_state(state: RecoveryState) -> dict[str, Any]:
    if not isinstance(state, RecoveryState):
        raise RecoveryStateCorruptError("state must be RecoveryState")
    encoded = _encode_typed(state, RecoveryState, path="state")
    _validate_state(state)
    if not isinstance(encoded, dict):
        raise RecoveryStateCorruptError("encoded recovery state must be an object")
    return encoded


def _decode_state(payload: Any) -> RecoveryState:
    state = _decode_typed(payload, RecoveryState, path="state")
    if not isinstance(state, RecoveryState):
        raise RecoveryStateCorruptError("decoded recovery state has the wrong type")
    _validate_state(state)
    return state


def _decode_recovery_envelope(
    envelope: Any,
    *,
    expected_symbol: str,
) -> RecoveryState:
    normalized_symbol = _normalize_symbol(expected_symbol)
    if not isinstance(envelope, MappingABC):
        raise RecoveryStateCorruptError(f"{RECOVERY_STATE_KEY} must be an object")
    _require_fields(envelope, {"schema_version", "state"}, path=RECOVERY_STATE_KEY)
    schema_version = envelope["schema_version"]
    if (
        type(schema_version) is not int
        or schema_version != RECOVERY_STATE_SCHEMA_VERSION
    ):
        raise RecoveryStateCorruptError(
            f"unsupported recovery state schema: {schema_version!r}"
        )
    state = _decode_state(envelope["state"])
    if state.symbol != normalized_symbol:
        raise RecoveryStateCorruptError(
            f"recovery symbol mismatch: expected {normalized_symbol}, "
            f"found {state.symbol}"
        )
    return state


def _recovery_envelope(state: RecoveryState) -> dict[str, Any]:
    return {
        "schema_version": RECOVERY_STATE_SCHEMA_VERSION,
        "state": _encode_state(state),
    }


def _decode_matching_recovery_slots(
    document: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> RecoveryState:
    """Require the two atomically-written slots to be valid and identical."""

    states: dict[str, RecoveryState] = {}
    failures: dict[str, str] = {}
    for key in _RECOVERY_STATE_SLOT_KEYS:
        if key not in document:
            failures[key] = "missing"
            continue
        try:
            states[key] = _decode_recovery_envelope(
                document[key],
                expected_symbol=expected_symbol,
            )
        except RecoveryStateCorruptError as exc:
            failures[key] = str(exc)
    if failures:
        details = "; ".join(f"{key}={value}" for key, value in failures.items())
        raise RecoveryStateCorruptError(
            f"recovery state mirror repair required: {details}"
        )
    primary = states[RECOVERY_STATE_KEY]
    mirror = states[RECOVERY_STATE_MIRROR_KEY]
    if primary != mirror:
        raise RecoveryStateCorruptError("recovery state slots diverge")
    canonical = _recovery_envelope(primary)
    for key in _RECOVERY_STATE_SLOT_KEYS:
        raw = _json_copy(document[key], path=f"control.{key}")
        if raw != canonical:
            raise RecoveryStateCorruptError(
                f"recovery state mirror repair required: {key} is not canonical"
            )
    return primary


def _resolve_recovery_slots_for_repair(
    document: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> tuple[RecoveryState, dict[str, Any], set[str]]:
    """Select exactly one logical state without guessing between valid revisions."""

    valid: dict[str, RecoveryState] = {}
    failures: dict[str, str] = {}
    for key in _RECOVERY_STATE_SLOT_KEYS:
        if key not in document:
            failures[key] = "missing"
            continue
        try:
            state = _decode_recovery_envelope(
                document[key],
                expected_symbol=expected_symbol,
            )
            if (
                _json_copy(document[key], path=f"control.{key}")
                != _recovery_envelope(state)
            ):
                raise RecoveryStateCorruptError("slot is not canonical")
            valid[key] = state
        except RecoveryStateCorruptError as exc:
            failures[key] = str(exc)
    if not valid:
        details = "; ".join(f"{key}={value}" for key, value in failures.items())
        raise RecoveryStateCorruptError(
            f"both recovery state slots are invalid: {details}"
        )
    states = tuple(valid.values())
    if len(states) == 2 and states[0] != states[1]:
        raise RecoveryStateCorruptError("recovery state slots diverge")
    state = states[0]
    canonical = _recovery_envelope(state)
    changed = {
        key
        for key in _RECOVERY_STATE_SLOT_KEYS
        if key not in valid
    }
    return state, canonical, changed


def decode_recovery_desired_profile_fields(
    envelope: Any,
    *,
    expected_symbol: str,
) -> dict[str, Any]:
    """Strictly decode an ownership envelope and return its desired flat fields."""

    state = _decode_recovery_envelope(
        envelope,
        expected_symbol=expected_symbol,
    )
    desired = _json_copy(
        state.desired_profile.fields,
        path="state.desired_profile.fields",
    )
    if not isinstance(desired, dict):
        raise RecoveryStateCorruptError(
            "state.desired_profile.fields must be an object"
        )
    return desired


def decode_recovery_state_slots(
    document: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> RecoveryState:
    """Validate both owner slots without treating repairable flat drift as state."""

    if not recovery_coordinator_registered(document):
        raise RecoveryStateUnavailableError(
            f"unregistered recovery symbol: {_normalize_symbol(expected_symbol)}"
        )
    return _decode_matching_recovery_slots(
        document,
        expected_symbol=_normalize_symbol(expected_symbol),
    )


class JsonRecoveryStore:
    """A lock-protected, one-control-document implementation of recovery CAS."""

    def __init__(
        self,
        control_path: str | Path,
        *,
        lock_timeout_seconds: float = 15.0,
    ) -> None:
        self.control_path = Path(control_path)
        self.lock_timeout_seconds = float(lock_timeout_seconds)

    @staticmethod
    def encode_state(state: RecoveryState) -> dict[str, Any]:
        return _encode_state(state)

    @staticmethod
    def decode_state(payload: Mapping[str, Any]) -> RecoveryState:
        return _decode_state(payload)

    def register_symbol(
        self,
        symbol: str,
        baseline: Mapping[str, Any],
        *,
        now: datetime,
    ) -> RecoveryState:
        normalized = _normalize_symbol(symbol)
        _encode_datetime(now, path="registration_now")
        with exclusive_control_lock(
            self.control_path, timeout_seconds=self.lock_timeout_seconds
        ):
            document = self._read_document(allow_missing=True)
            if recovery_coordinator_registered(document):
                existing = self._decode_document_state(document, normalized)
                raise ValueError(f"symbol already registered: {existing.symbol}")
            for source, value in (
                (
                    "control",
                    document.get(
                        "best_quote_maker_volume_reduce_freeze_enabled"
                    ),
                ),
                (
                    "baseline",
                    baseline.get(
                        "best_quote_maker_volume_reduce_freeze_enabled"
                    ),
                ),
            ):
                if value is not None and type(value) is not bool:
                    raise ValueError(
                        "cannot register recovery while frozen inventory "
                        f"creation config is invalid in {source}"
                    )
                if value is True:
                    raise ValueError(
                        "cannot register recovery while frozen inventory "
                        f"creation is enabled in {source}"
                    )
            state_suffix = "_loop_runner_control.json"
            if self.control_path.name.endswith(state_suffix):
                runner_state_path = self.control_path.with_name(
                    self.control_path.name[: -len(state_suffix)]
                    + "_loop_state.json"
                )
                try:
                    runner_state_raw = runner_state_path.read_text(encoding="utf-8")
                except FileNotFoundError:
                    runner_state = {}
                except OSError as exc:
                    raise ValueError(
                        "cannot register recovery while runner state is unreadable"
                    ) from exc
                else:
                    try:
                        runner_state = json.loads(runner_state_raw)
                    except (UnicodeError, json.JSONDecodeError) as exc:
                        raise ValueError(
                            "cannot register recovery while runner state is unreadable"
                        ) from exc
                    if not isinstance(runner_state, MappingABC):
                        raise ValueError(
                            "cannot register recovery while runner state is unreadable"
                        )
                frozen = runner_state.get("best_quote_frozen_inventory")
                if isinstance(frozen, MappingABC):
                    quantities: list[float] = []
                    for key in ("long_qty", "short_qty"):
                        try:
                            quantities.append(float(frozen.get(key) or 0.0))
                        except (TypeError, ValueError):
                            quantities.append(math.inf)
                    for key in ("long_lots", "short_lots"):
                        lots = frozen.get(key)
                        if isinstance(lots, list):
                            for lot in lots:
                                if not isinstance(lot, MappingABC):
                                    quantities.append(math.inf)
                                    continue
                                try:
                                    quantities.append(float(lot.get("qty") or 0.0))
                                except (TypeError, ValueError):
                                    quantities.append(math.inf)
                    if any(
                        not math.isfinite(value) or value > 1e-12
                        for value in quantities
                    ):
                        raise ValueError(
                            "cannot register recovery while frozen inventory exists"
                        )
            state = RecoveryState.initial(normalized, baseline, now=now)
            self._write_state(document, current=None, next_state=state)
            return state

    def read(self, symbol: str) -> RecoveryState:
        normalized = _normalize_symbol(symbol)
        with exclusive_control_lock(
            self.control_path, timeout_seconds=self.lock_timeout_seconds
        ):
            return self._decode_document_state(
                self._read_document(allow_missing=False), normalized
            )

    def reconcile_flat_profile(
        self,
        symbol: str,
        *,
        dry_run: bool = False,
    ) -> tuple[RecoveryState, tuple[str, ...]]:
        """Restore flat managed fields from the validated embedded owner.

        This is a one-round local repair boundary for a registered symbol.  It
        never changes the embedded recovery revision or any unrelated control
        field; the next coordinator round performs any lifecycle transition.
        """

        normalized = _normalize_symbol(symbol)
        with exclusive_control_lock(
            self.control_path, timeout_seconds=self.lock_timeout_seconds
        ):
            document = self._read_document(allow_missing=False)
            if not recovery_coordinator_registered(document):
                raise RecoveryStateUnavailableError(
                    f"unregistered recovery symbol: {normalized}"
                )
            state, canonical_envelope, slot_changes = (
                _resolve_recovery_slots_for_repair(
                    document,
                    expected_symbol=normalized,
                )
            )
            expected = _json_copy(
                state.desired_profile.fields,
                path="state.desired_profile.fields",
            )
            changed: set[str] = set(slot_changes)
            for key, expected_value in expected.items():
                try:
                    actual_value = _json_copy(
                        document[key], path=f"control.{key}"
                    )
                except (KeyError, RecoveryStateCorruptError):
                    changed.add(key)
                    continue
                if actual_value != expected_value:
                    changed.add(key)
            unexpected = {
                key
                for key in document
                if _is_managed_recovery_key(key) and key not in expected
            }
            changed.update(unexpected)
            if not changed or dry_run:
                return state, tuple(sorted(changed))

            repaired = dict(document)
            for key in unexpected:
                repaired.pop(key, None)
            repaired.update(expected)
            for key in _RECOVERY_STATE_SLOT_KEYS:
                repaired[key] = _json_copy(
                    canonical_envelope,
                    path=f"control.{key}",
                )
            write_control_json_atomically(self.control_path, repaired)
            return state, tuple(sorted(changed))

    def compare_and_swap(
        self,
        symbol: str,
        *,
        expected_revision: int,
        next_state: RecoveryState,
    ) -> None:
        normalized = _normalize_symbol(symbol)
        if type(expected_revision) is not int or expected_revision < 0:
            raise ValueError("expected_revision must be a non-negative integer")
        if next_state.symbol != normalized:
            raise ValueError("next recovery state symbol does not match store symbol")
        if next_state.document_revision != expected_revision + 1:
            raise ValueError("one round must increment document_revision exactly once")
        _encode_state(next_state)
        with exclusive_control_lock(
            self.control_path, timeout_seconds=self.lock_timeout_seconds
        ):
            document = self._read_document(allow_missing=False)
            current = self._decode_document_state(document, normalized)
            if current.document_revision != expected_revision:
                raise RuntimeError(
                    f"recovery revision conflict for {normalized}: "
                    f"expected {expected_revision}, "
                    f"actual {current.document_revision}"
                )
            self._write_state(document, current=current, next_state=next_state)

    def _read_document(self, *, allow_missing: bool) -> dict[str, Any]:
        try:
            raw = self.control_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            if allow_missing:
                return {}
            raise RecoveryStateUnavailableError(
                f"recovery control document is missing: {self.control_path}"
            ) from None
        except OSError as exc:
            raise RecoveryStateCorruptError(
                f"cannot read recovery control document: {self.control_path}"
            ) from exc
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RecoveryStateCorruptError(
                f"recovery control document is invalid JSON: {self.control_path}"
            ) from exc
        if not isinstance(payload, dict):
            raise RecoveryStateCorruptError(
                "recovery control document must contain a JSON object"
            )
        return payload

    @staticmethod
    def _decode_document_state(
        document: Mapping[str, Any], expected_symbol: str
    ) -> RecoveryState:
        if not recovery_coordinator_registered(document):
            raise RecoveryStateUnavailableError(
                f"unregistered recovery symbol: {expected_symbol}"
            )
        state = _decode_matching_recovery_slots(
            document,
            expected_symbol=expected_symbol,
        )
        expected_flat = _json_copy(
            state.desired_profile.fields,
            path="state.desired_profile.fields",
        )
        for key, expected_value in expected_flat.items():
            if key not in document:
                raise RecoveryStateCorruptError(
                    f"flat managed control drift: missing {key}"
                )
            actual_value = _json_copy(document[key], path=f"control.{key}")
            if actual_value != expected_value:
                raise RecoveryStateCorruptError(
                    f"flat managed control drift: {key} does not match "
                    "embedded desired_profile"
                )
        unexpected = sorted(
            key
            for key in document
            if _is_managed_recovery_key(key) and key not in expected_flat
        )
        if unexpected:
            raise RecoveryStateCorruptError(
                "unexpected managed recovery field: " + ", ".join(unexpected)
            )
        return state

    def _write_state(
        self,
        document: Mapping[str, Any],
        *,
        current: RecoveryState | None,
        next_state: RecoveryState,
    ) -> None:
        encoded = _encode_state(next_state)
        updated = dict(document)
        next_managed_keys = set(next_state.desired_profile.fields)
        for key in tuple(updated):
            if _is_managed_recovery_key(key) and key not in next_managed_keys:
                updated.pop(key, None)
        if current is not None:
            for key in current.desired_profile.fields:
                updated.pop(key, None)
        updated.update(
            _json_copy(next_state.desired_profile.fields, path="desired_profile.fields")
        )
        envelope = {
            "schema_version": RECOVERY_STATE_SCHEMA_VERSION,
            "state": encoded,
        }
        updated[RECOVERY_STATE_KEY] = _json_copy(
            envelope, path=RECOVERY_STATE_KEY
        )
        updated[RECOVERY_STATE_MIRROR_KEY] = _json_copy(
            envelope, path=RECOVERY_STATE_MIRROR_KEY
        )
        write_control_json_atomically(self.control_path, updated)


def decode_recovery_control_state(
    document: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> RecoveryState:
    """Strictly validate one already-loaded recovery control document."""

    return JsonRecoveryStore._decode_document_state(
        document,
        _normalize_symbol(expected_symbol),
    )
