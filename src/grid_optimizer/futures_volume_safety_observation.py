"""Typed, expiring market-volume observations for registered futures runners."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping


VOLUME_SAFETY_OBSERVATION_SCHEMA = "futures_volume_safety_observation_v1"
VOLUME_SAFETY_OBSERVATION_SOURCE = "web_volume_trigger"
VOLUME_SAFETY_OBSERVATION_MAX_AGE = timedelta(seconds=120)
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
        "current_quote_volume",
        "stop_threshold",
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


def _finite_nonnegative(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field} must be finite and >= 0")
    return parsed


@dataclass(frozen=True)
class VolumeSafetyObservation:
    symbol: str
    run_contract_id: str
    generation: int
    observed_at: datetime
    hard_expires_at: datetime
    active: bool
    reason: str
    window: str
    window_minutes: int
    current_quote_volume: float
    stop_threshold: float

    def __post_init__(self) -> None:
        if self.symbol != self.symbol.upper().strip() or not self.symbol:
            raise ValueError("volume safety observation symbol must be canonical")
        if not self.run_contract_id.strip():
            raise ValueError("volume safety observation run contract is required")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("volume safety observation generation is invalid")
        _aware(self.observed_at, "observed_at")
        _aware(self.hard_expires_at, "hard_expires_at")
        if self.hard_expires_at != (
            self.observed_at + VOLUME_SAFETY_OBSERVATION_MAX_AGE
        ):
            raise ValueError("volume safety observation hard expiry is invalid")
        if type(self.active) is not bool:
            raise ValueError("volume safety observation active must be boolean")
        if not self.window.strip() or type(self.window_minutes) is not int or self.window_minutes <= 0:
            raise ValueError("volume safety observation window is invalid")
        _finite_nonnegative(self.current_quote_volume, "current_quote_volume")
        if _finite_nonnegative(self.stop_threshold, "stop_threshold") <= 0:
            raise ValueError("stop_threshold must be > 0")
        expected_active = self.current_quote_volume < self.stop_threshold
        if self.active is not expected_active:
            raise ValueError("volume safety observation predicate mismatch")
        expected_reason = (
            "volume_below_stop_threshold"
            if expected_active
            else "volume_at_or_above_stop_threshold"
        )
        if self.reason != expected_reason:
            raise ValueError("volume safety observation reason mismatch")

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
            "schema": VOLUME_SAFETY_OBSERVATION_SCHEMA,
            "source": VOLUME_SAFETY_OBSERVATION_SOURCE,
            "symbol": self.symbol,
            "run_contract_id": self.run_contract_id,
            "generation": self.generation,
            "observed_at": self.observed_at.isoformat(),
            "hard_expires_at": self.hard_expires_at.isoformat(),
            "active": self.active,
            "reason": self.reason,
            "window": self.window,
            "window_minutes": self.window_minutes,
            "current_quote_volume": self.current_quote_volume,
            "stop_threshold": self.stop_threshold,
        }


def build_volume_safety_observation(
    *,
    symbol: str,
    run_contract_id: str,
    generation: int,
    observed_at: datetime,
    window: str,
    window_minutes: int,
    current_quote_volume: float,
    stop_threshold: float,
) -> VolumeSafetyObservation:
    current = _finite_nonnegative(current_quote_volume, "current_quote_volume")
    threshold = _finite_nonnegative(stop_threshold, "stop_threshold")
    active = current < threshold
    return VolumeSafetyObservation(
        symbol=str(symbol).upper().strip(),
        run_contract_id=str(run_contract_id).strip(),
        generation=generation,
        observed_at=_aware(observed_at, "observed_at"),
        hard_expires_at=observed_at + VOLUME_SAFETY_OBSERVATION_MAX_AGE,
        active=active,
        reason=(
            "volume_below_stop_threshold"
            if active
            else "volume_at_or_above_stop_threshold"
        ),
        window=str(window).strip(),
        window_minutes=window_minutes,
        current_quote_volume=current,
        stop_threshold=threshold,
    )


def decode_volume_safety_observation(
    raw: Mapping[str, Any],
) -> VolumeSafetyObservation:
    if set(raw) != _OBSERVATION_KEYS:
        raise ValueError("volume safety observation fields are invalid")
    if raw.get("schema") != VOLUME_SAFETY_OBSERVATION_SCHEMA:
        raise ValueError("volume safety observation schema is invalid")
    if raw.get("source") != VOLUME_SAFETY_OBSERVATION_SOURCE:
        raise ValueError("volume safety observation source is invalid")
    return VolumeSafetyObservation(
        symbol=str(raw.get("symbol") or ""),
        run_contract_id=str(raw.get("run_contract_id") or ""),
        generation=raw.get("generation"),
        observed_at=_parse_time(raw.get("observed_at"), "observed_at"),
        hard_expires_at=_parse_time(raw.get("hard_expires_at"), "hard_expires_at"),
        active=raw.get("active"),
        reason=str(raw.get("reason") or ""),
        window=str(raw.get("window") or ""),
        window_minutes=raw.get("window_minutes"),
        current_quote_volume=_finite_nonnegative(
            raw.get("current_quote_volume"), "current_quote_volume"
        ),
        stop_threshold=_finite_nonnegative(raw.get("stop_threshold"), "stop_threshold"),
    )


def validate_volume_safety_observation(
    raw: Mapping[str, Any],
    *,
    expected_symbol: str,
    expected_run_contract_id: str,
    expected_generation: int,
    now: datetime,
) -> VolumeSafetyObservation | None:
    current = _aware(now, "now")
    observation = decode_volume_safety_observation(raw)
    if (
        observation.symbol != expected_symbol.upper().strip()
        or observation.run_contract_id != expected_run_contract_id
        or observation.generation != expected_generation
    ):
        raise ValueError("volume safety observation binding mismatch")
    if current < observation.observed_at:
        raise ValueError("volume safety observation is from the future")
    if current >= observation.hard_expires_at:
        return None
    return observation
