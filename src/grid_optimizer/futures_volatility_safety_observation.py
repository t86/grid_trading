"""Typed, expiring Web volatility observations for registered futures runners."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping


VOLATILITY_SAFETY_OBSERVATION_SCHEMA = (
    "futures_volatility_safety_observation_v1"
)
VOLATILITY_SAFETY_OBSERVATION_SOURCE = "web_volatility_trigger"
VOLATILITY_SAFETY_OBSERVATION_MAX_AGE = timedelta(seconds=120)
_OBSERVATION_KEYS = frozenset(
    {
        "schema",
        "source",
        "symbol",
        "run_contract_id",
        "generation",
        "observed_at",
        "hard_expires_at",
        "active",
        "reason",
        "window",
        "window_minutes",
        "current_amplitude_ratio",
        "current_return_ratio",
        "amplitude_threshold",
        "abs_return_threshold",
        "matched_reasons",
    }
)


def _aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must include timezone information")
    return value


def _parse_time(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid ISO timestamp") from exc
    return _aware(parsed, field)


def _finite(value: Any, field: str, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed) or (nonnegative and parsed < 0):
        suffix = " and >= 0" if nonnegative else ""
        raise ValueError(f"{field} must be finite{suffix}")
    return parsed


def _optional_threshold(value: Any, field: str) -> float | None:
    if value is None:
        return None
    return _finite(value, field, nonnegative=True)


def _matched_reasons(
    *,
    current_amplitude_ratio: float,
    current_return_ratio: float,
    amplitude_threshold: float | None,
    abs_return_threshold: float | None,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if (
        amplitude_threshold is not None
        and current_amplitude_ratio >= amplitude_threshold
    ):
        reasons.append("amplitude_above_threshold")
    if (
        abs_return_threshold is not None
        and abs(current_return_ratio) >= abs_return_threshold
    ):
        reasons.append("abs_return_above_threshold")
    return tuple(reasons)


@dataclass(frozen=True)
class VolatilitySafetyObservation:
    symbol: str
    run_contract_id: str
    generation: int
    observed_at: datetime
    hard_expires_at: datetime
    active: bool
    reason: str
    window: str
    window_minutes: int
    current_amplitude_ratio: float
    current_return_ratio: float
    amplitude_threshold: float | None
    abs_return_threshold: float | None
    matched_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.symbol != self.symbol.upper().strip() or not self.symbol:
            raise ValueError("volatility safety observation symbol must be canonical")
        if not self.run_contract_id.strip():
            raise ValueError("volatility safety observation run contract is required")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("volatility safety observation generation is invalid")
        _aware(self.observed_at, "observed_at")
        _aware(self.hard_expires_at, "hard_expires_at")
        if self.hard_expires_at != (
            self.observed_at + VOLATILITY_SAFETY_OBSERVATION_MAX_AGE
        ):
            raise ValueError("volatility safety observation hard expiry is invalid")
        if type(self.active) is not bool:
            raise ValueError("volatility safety observation active must be boolean")
        if (
            not self.window.strip()
            or type(self.window_minutes) is not int
            or self.window_minutes <= 0
        ):
            raise ValueError("volatility safety observation window is invalid")
        current_amplitude = _finite(
            self.current_amplitude_ratio,
            "current_amplitude_ratio",
            nonnegative=True,
        )
        current_return = _finite(self.current_return_ratio, "current_return_ratio")
        amplitude_threshold = _optional_threshold(
            self.amplitude_threshold,
            "amplitude_threshold",
        )
        return_threshold = _optional_threshold(
            self.abs_return_threshold,
            "abs_return_threshold",
        )
        expected_reasons = _matched_reasons(
            current_amplitude_ratio=current_amplitude,
            current_return_ratio=current_return,
            amplitude_threshold=amplitude_threshold,
            abs_return_threshold=return_threshold,
        )
        if self.matched_reasons != expected_reasons:
            raise ValueError("volatility safety observation reasons mismatch")
        expected_active = bool(expected_reasons)
        if self.active is not expected_active:
            raise ValueError("volatility safety observation predicate mismatch")
        expected_reason = (
            "volatility_entry_pause" if expected_active else "volatility_clear"
        )
        if self.reason != expected_reason:
            raise ValueError("volatility safety observation reason mismatch")

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
            "schema": VOLATILITY_SAFETY_OBSERVATION_SCHEMA,
            "source": VOLATILITY_SAFETY_OBSERVATION_SOURCE,
            "symbol": self.symbol,
            "run_contract_id": self.run_contract_id,
            "generation": self.generation,
            "observed_at": self.observed_at.isoformat(),
            "hard_expires_at": self.hard_expires_at.isoformat(),
            "active": self.active,
            "reason": self.reason,
            "window": self.window,
            "window_minutes": self.window_minutes,
            "current_amplitude_ratio": self.current_amplitude_ratio,
            "current_return_ratio": self.current_return_ratio,
            "amplitude_threshold": self.amplitude_threshold,
            "abs_return_threshold": self.abs_return_threshold,
            "matched_reasons": list(self.matched_reasons),
        }


def build_volatility_safety_observation(
    *,
    symbol: str,
    run_contract_id: str,
    generation: int,
    observed_at: datetime,
    window: str,
    window_minutes: int,
    current_amplitude_ratio: float,
    current_return_ratio: float,
    amplitude_threshold: float | None,
    abs_return_threshold: float | None,
) -> VolatilitySafetyObservation:
    amplitude = _finite(
        current_amplitude_ratio,
        "current_amplitude_ratio",
        nonnegative=True,
    )
    return_ratio = _finite(current_return_ratio, "current_return_ratio")
    amplitude_limit = _optional_threshold(
        amplitude_threshold,
        "amplitude_threshold",
    )
    return_limit = _optional_threshold(
        abs_return_threshold,
        "abs_return_threshold",
    )
    reasons = _matched_reasons(
        current_amplitude_ratio=amplitude,
        current_return_ratio=return_ratio,
        amplitude_threshold=amplitude_limit,
        abs_return_threshold=return_limit,
    )
    active = bool(reasons)
    return VolatilitySafetyObservation(
        symbol=str(symbol).upper().strip(),
        run_contract_id=str(run_contract_id).strip(),
        generation=generation,
        observed_at=_aware(observed_at, "observed_at"),
        hard_expires_at=observed_at + VOLATILITY_SAFETY_OBSERVATION_MAX_AGE,
        active=active,
        reason="volatility_entry_pause" if active else "volatility_clear",
        window=str(window).strip(),
        window_minutes=window_minutes,
        current_amplitude_ratio=amplitude,
        current_return_ratio=return_ratio,
        amplitude_threshold=amplitude_limit,
        abs_return_threshold=return_limit,
        matched_reasons=reasons,
    )


def decode_volatility_safety_observation(
    raw: Mapping[str, Any],
) -> VolatilitySafetyObservation:
    if set(raw) != _OBSERVATION_KEYS:
        raise ValueError("volatility safety observation fields are invalid")
    if raw.get("schema") != VOLATILITY_SAFETY_OBSERVATION_SCHEMA:
        raise ValueError("volatility safety observation schema is invalid")
    if raw.get("source") != VOLATILITY_SAFETY_OBSERVATION_SOURCE:
        raise ValueError("volatility safety observation source is invalid")
    matched_reasons = raw.get("matched_reasons")
    if not isinstance(matched_reasons, list) or any(
        not isinstance(item, str) for item in matched_reasons
    ):
        raise ValueError("volatility safety observation reasons are invalid")
    return VolatilitySafetyObservation(
        symbol=str(raw.get("symbol") or ""),
        run_contract_id=str(raw.get("run_contract_id") or ""),
        generation=raw.get("generation"),
        observed_at=_parse_time(raw.get("observed_at"), "observed_at"),
        hard_expires_at=_parse_time(raw.get("hard_expires_at"), "hard_expires_at"),
        active=raw.get("active"),
        reason=str(raw.get("reason") or ""),
        window=str(raw.get("window") or ""),
        window_minutes=raw.get("window_minutes"),
        current_amplitude_ratio=_finite(
            raw.get("current_amplitude_ratio"),
            "current_amplitude_ratio",
            nonnegative=True,
        ),
        current_return_ratio=_finite(
            raw.get("current_return_ratio"),
            "current_return_ratio",
        ),
        amplitude_threshold=_optional_threshold(
            raw.get("amplitude_threshold"),
            "amplitude_threshold",
        ),
        abs_return_threshold=_optional_threshold(
            raw.get("abs_return_threshold"),
            "abs_return_threshold",
        ),
        matched_reasons=tuple(matched_reasons),
    )


def validate_volatility_safety_observation(
    raw: Mapping[str, Any],
    *,
    expected_symbol: str,
    expected_run_contract_id: str,
    expected_generation: int,
    now: datetime,
) -> VolatilitySafetyObservation | None:
    current = _aware(now, "now")
    observation = decode_volatility_safety_observation(raw)
    if (
        observation.symbol != expected_symbol.upper().strip()
        or observation.run_contract_id != expected_run_contract_id
        or observation.generation != expected_generation
    ):
        raise ValueError("volatility safety observation binding mismatch")
    if current < observation.observed_at:
        raise ValueError("volatility safety observation is from the future")
    if current >= observation.hard_expires_at:
        return None
    return observation
