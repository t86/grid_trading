"""Typed, local safety observations consumed by the recovery coordinator.

The runner may observe a safety condition, but a registered symbol must not
turn that observation into a second cancel/flatten/restart executor.  This
module keeps the observation bound to the exact run contract and recovery
decision that produced it so the BQ coordinator can remain the sole actuator.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from .futures_recovery_coordinator import ActionId, Side


RUNTIME_SAFETY_SIGNAL_KEY = "futures_recovery_runtime_safety_signal"
RUNTIME_SAFETY_SIGNAL_SCHEMA = "futures_recovery_runtime_safety_signal_v1"
RUNTIME_SAFETY_SIGNAL_MIN_TTL = timedelta(seconds=120)
RUNTIME_SAFETY_SIGNAL_MAX_TTL = timedelta(minutes=15)
_ALLOWED_ACTIONS = frozenset(
    {
        ActionId.SAFETY_CONVERGE,
        ActionId.INVENTORY_RECOVER,
        ActionId.RUNNER_RECOVER,
    }
)
_SIGNAL_KEYS = frozenset(
    {
        "schema",
        "symbol",
        "run_contract_id",
        "generation",
        "decision_id",
        "observed_at",
        "hard_expires_at",
        "conditions",
    }
)
_CONDITION_KEYS = frozenset(
    {
        "source",
        "action_id",
        "reason",
        "side",
        "observed_at",
        "hard_expires_at",
        "details_digest",
    }
)


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone information")
    return value


def _parse_time(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO timestamp") from exc
    return _aware(parsed, field_name)


def details_digest(details: Mapping[str, Any]) -> str:
    try:
        encoded = json.dumps(
            dict(details),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("runtime safety details must be canonical JSON") from exc
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class RuntimeSafetyCondition:
    source: str
    action_id: ActionId
    reason: str
    side: Side | None
    observed_at: datetime
    hard_expires_at: datetime
    details_digest: str

    def __post_init__(self) -> None:
        if not self.source.strip():
            raise ValueError("runtime safety condition source is required")
        if self.action_id not in _ALLOWED_ACTIONS:
            raise ValueError("runtime safety condition action is unsupported")
        if not self.reason.strip():
            raise ValueError("runtime safety condition reason is required")
        _aware(self.observed_at, "condition.observed_at")
        _aware(self.hard_expires_at, "condition.hard_expires_at")
        ttl = self.hard_expires_at - self.observed_at
        if ttl < RUNTIME_SAFETY_SIGNAL_MIN_TTL:
            raise ValueError("runtime safety condition TTL is too short")
        if ttl > RUNTIME_SAFETY_SIGNAL_MAX_TTL:
            raise ValueError("runtime safety condition TTL exceeds maximum")
        if self.action_id is ActionId.INVENTORY_RECOVER:
            if self.side is None:
                raise ValueError("inventory runtime safety condition requires side")
        elif self.side is not None:
            raise ValueError("only inventory runtime safety condition may carry side")
        if not (
            self.details_digest.startswith("sha256:")
            and len(self.details_digest) == len("sha256:") + 64
            and all(
                character in "0123456789abcdef"
                for character in self.details_digest.removeprefix("sha256:")
            )
        ):
            raise ValueError("runtime safety condition details digest is invalid")

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "action_id": self.action_id.value,
            "reason": self.reason,
            "side": self.side.value if self.side is not None else None,
            "observed_at": self.observed_at.isoformat(),
            "hard_expires_at": self.hard_expires_at.isoformat(),
            "details_digest": self.details_digest,
        }


@dataclass(frozen=True)
class RuntimeSafetySignal:
    symbol: str
    run_contract_id: str
    generation: int
    decision_id: str | None
    observed_at: datetime
    hard_expires_at: datetime
    conditions: tuple[RuntimeSafetyCondition, ...]

    def __post_init__(self) -> None:
        if self.symbol != self.symbol.upper().strip() or not self.symbol:
            raise ValueError("runtime safety signal symbol must be canonical")
        if not self.run_contract_id.strip():
            raise ValueError("runtime safety signal run contract is required")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("runtime safety signal generation is invalid")
        if self.decision_id is not None and not self.decision_id.strip():
            raise ValueError("runtime safety signal decision_id is invalid")
        _aware(self.observed_at, "signal.observed_at")
        _aware(self.hard_expires_at, "signal.hard_expires_at")
        if not self.conditions:
            raise ValueError("runtime safety signal requires at least one condition")
        keys = [(item.source, item.reason) for item in self.conditions]
        if keys != sorted(set(keys)):
            raise ValueError("runtime safety signal conditions must be unique and sorted")
        if any(item.observed_at > self.observed_at for item in self.conditions):
            raise ValueError("condition cannot be newer than signal")
        expected_expiry = max(
            item.hard_expires_at for item in self.conditions
        )
        if self.hard_expires_at != expected_expiry:
            raise ValueError("runtime safety signal hard expiry is invalid")

    @property
    def fingerprint(self) -> str:
        encoded = json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    @property
    def observation_seq(self) -> int:
        return int(self.observed_at.timestamp() * 1_000_000)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": RUNTIME_SAFETY_SIGNAL_SCHEMA,
            "symbol": self.symbol,
            "run_contract_id": self.run_contract_id,
            "generation": self.generation,
            "decision_id": self.decision_id,
            "observed_at": self.observed_at.isoformat(),
            "hard_expires_at": self.hard_expires_at.isoformat(),
            "conditions": [item.as_dict() for item in self.conditions],
        }


def runtime_safety_condition(
    *,
    source: str,
    action_id: ActionId,
    reason: str,
    observed_at: datetime,
    details: Mapping[str, Any],
    side: Side | None = None,
    ttl: timedelta = RUNTIME_SAFETY_SIGNAL_MIN_TTL,
) -> RuntimeSafetyCondition:
    if ttl < RUNTIME_SAFETY_SIGNAL_MIN_TTL:
        raise ValueError("runtime safety condition TTL is too short")
    if ttl > RUNTIME_SAFETY_SIGNAL_MAX_TTL:
        raise ValueError("runtime safety condition TTL exceeds maximum")
    return RuntimeSafetyCondition(
        source=str(source).strip(),
        action_id=action_id,
        reason=str(reason).strip(),
        side=side,
        observed_at=_aware(observed_at, "condition.observed_at"),
        hard_expires_at=observed_at + ttl,
        details_digest=details_digest(details),
    )


def decode_runtime_safety_signal(raw: Mapping[str, Any]) -> RuntimeSafetySignal:
    if set(raw) != _SIGNAL_KEYS:
        raise ValueError("runtime safety signal fields are invalid")
    if raw.get("schema") != RUNTIME_SAFETY_SIGNAL_SCHEMA:
        raise ValueError("runtime safety signal schema is invalid")
    raw_conditions = raw.get("conditions")
    if not isinstance(raw_conditions, list):
        raise ValueError("runtime safety signal conditions are invalid")
    conditions: list[RuntimeSafetyCondition] = []
    for raw_condition in raw_conditions:
        if not isinstance(raw_condition, Mapping) or set(raw_condition) != _CONDITION_KEYS:
            raise ValueError("runtime safety condition fields are invalid")
        try:
            action_id = ActionId(str(raw_condition.get("action_id") or ""))
            side = (
                Side(str(raw_condition.get("side")))
                if raw_condition.get("side") is not None
                else None
            )
        except ValueError as exc:
            raise ValueError("runtime safety condition enum is invalid") from exc
        conditions.append(
            RuntimeSafetyCondition(
                source=str(raw_condition.get("source") or ""),
                action_id=action_id,
                reason=str(raw_condition.get("reason") or ""),
                side=side,
                observed_at=_parse_time(
                    raw_condition.get("observed_at"),
                    "condition.observed_at",
                ),
                hard_expires_at=_parse_time(
                    raw_condition.get("hard_expires_at"),
                    "condition.hard_expires_at",
                ),
                details_digest=str(raw_condition.get("details_digest") or ""),
            )
        )
    decision_id = raw.get("decision_id")
    if decision_id is not None and not isinstance(decision_id, str):
        raise ValueError("runtime safety signal decision_id is invalid")
    return RuntimeSafetySignal(
        symbol=str(raw.get("symbol") or ""),
        run_contract_id=str(raw.get("run_contract_id") or ""),
        generation=raw.get("generation"),
        decision_id=decision_id,
        observed_at=_parse_time(raw.get("observed_at"), "signal.observed_at"),
        hard_expires_at=_parse_time(
            raw.get("hard_expires_at"),
            "signal.hard_expires_at",
        ),
        conditions=tuple(conditions),
    )


def validate_runtime_safety_signal(
    raw: Mapping[str, Any],
    *,
    expected_symbol: str,
    expected_run_contract_id: str,
    expected_generation: int,
    expected_decision_id: str | None,
    now: datetime,
) -> RuntimeSafetySignal | None:
    current = _aware(now, "now")
    signal = decode_runtime_safety_signal(raw)
    if (
        signal.symbol != expected_symbol.upper().strip()
        or signal.run_contract_id != expected_run_contract_id
        or signal.generation != expected_generation
        or signal.decision_id != expected_decision_id
    ):
        raise ValueError("runtime safety signal binding mismatch")
    if current < signal.observed_at:
        raise ValueError("runtime safety signal is from the future")
    if current >= signal.hard_expires_at:
        return None
    if any(item.observed_at > current for item in signal.conditions):
        raise ValueError("runtime safety signal contains future conditions")
    fresh = tuple(
        item for item in signal.conditions if current < item.hard_expires_at
    )
    if not fresh:
        return None
    if fresh == signal.conditions:
        return signal
    return RuntimeSafetySignal(
        symbol=signal.symbol,
        run_contract_id=signal.run_contract_id,
        generation=signal.generation,
        decision_id=signal.decision_id,
        observed_at=max(item.observed_at for item in fresh),
        hard_expires_at=max(item.hard_expires_at for item in fresh),
        conditions=fresh,
    )


def replace_runtime_safety_source(
    raw: Mapping[str, Any] | None,
    *,
    symbol: str,
    run_contract_id: str,
    generation: int,
    decision_id: str | None,
    source: str,
    conditions: Sequence[RuntimeSafetyCondition],
    now: datetime,
) -> RuntimeSafetySignal | None:
    """Replace one observer's conditions and retain only fresh peer sources."""

    current = _aware(now, "now")
    retained: list[RuntimeSafetyCondition] = []
    if isinstance(raw, Mapping):
        try:
            existing = decode_runtime_safety_signal(raw)
        except ValueError:
            existing = None
        if (
            existing is not None
            and existing.symbol == symbol.upper().strip()
            and existing.run_contract_id == run_contract_id
            and existing.generation == generation
            and existing.decision_id == decision_id
        ):
            retained = [
                item
                for item in existing.conditions
                if item.source != source
                and item.observed_at <= current < item.hard_expires_at
            ]
    merged = sorted(
        [*retained, *conditions],
        key=lambda item: (item.source, item.reason),
    )
    if not merged:
        return None
    observed_at = max(item.observed_at for item in merged)
    hard_expires_at = max(item.hard_expires_at for item in merged)
    return RuntimeSafetySignal(
        symbol=symbol.upper().strip(),
        run_contract_id=run_contract_id,
        generation=generation,
        decision_id=decision_id,
        observed_at=observed_at,
        hard_expires_at=hard_expires_at,
        conditions=tuple(merged),
    )
