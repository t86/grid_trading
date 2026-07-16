"""Pure validation for durable futures terminal ownership artifacts.

The loop runner, recovery guard and watchdog must agree on whether a lifecycle
intent is trustworthy before any of them accepts, delegates or resumes it.
This module deliberately has no runner or exchange dependencies so every
consumer can apply the same proof checks without importing ``loop_runner``.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .futures_run_lifecycle import (
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from .futures_terminal_drain import (
    DrainStage,
    terminal_drain_state_from_dict,
)


TERMINAL_INTENT_SCHEMA = "futures_lifecycle_intent_v2"
TERMINAL_INTENT_ACTIVE_STATUSES = frozenset(
    {"pending", "accepted", "executing", "exit_blocked"}
)
TERMINAL_INTENT_COMPLETED_STATUSES = frozenset(
    {"completed", "stopped_clean", "stopped_preserved"}
)
TERMINAL_INTENT_STATUSES = (
    TERMINAL_INTENT_ACTIVE_STATUSES | TERMINAL_INTENT_COMPLETED_STATUSES
)
TERMINAL_INTENT_TRIGGER_REASONS = frozenset(
    {"target_reached", "target_unmet_deadline", "wear_limit_breached"}
)

TERMINAL_DRAIN_SCHEMA = "futures_terminal_drain_runtime_v1"
TERMINAL_DRAIN_COMPLETED_EXIT_STATUSES = frozenset(
    {"stopped_clean", "stopped_preserved"}
)


class TerminalIntentValidationError(ValueError):
    """A stable fail-closed reason shared by all intent consumers."""

    def __init__(self, code: str, detail: str | None = None) -> None:
        self.code = str(code)
        self.detail = str(detail) if detail else None
        super().__init__(
            self.code if self.detail is None else f"{self.code}: {self.detail}"
        )


@dataclass(frozen=True)
class ValidatedTerminalIntent:
    symbol: str
    source: str
    intent_id: str
    status: str
    trigger_reason: str
    requested_at: datetime
    run_contract_id: str
    run_contract_snapshot: dict[str, Any]
    gross_notional: float
    target: float
    realized_pnl: float
    wear_per_10k: float
    trade_count: int
    window_start: datetime
    window_end: datetime
    query_end: datetime


def _invalid(code: str, detail: str | None = None) -> None:
    raise TerminalIntentValidationError(code, detail)


def _aware_timestamp(value: Any, *, code: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        _invalid(code)
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        _invalid(code)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _invalid(code)
    return parsed.astimezone(timezone.utc)


def _finite_number(value: Any, *, code: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid(code)
    parsed = float(value)
    if not math.isfinite(parsed):
        _invalid(code)
    return parsed


def terminal_intent_id(
    *,
    symbol: str,
    source: str,
    trigger_reason: str,
    run_contract_id: str,
) -> str:
    identity = json.dumps(
        {
            "symbol": symbol,
            "source": source,
            "trigger_reason": trigger_reason,
            "run_contract_id": run_contract_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{symbol}-{source}-{trigger_reason}-{digest}"


def validate_terminal_intent(
    intent: Mapping[str, Any],
    *,
    expected_symbol: str,
) -> ValidatedTerminalIntent:
    """Validate a complete terminal fact without mutating state or doing I/O."""

    if not isinstance(intent, Mapping):
        _invalid("terminal_intent_unreadable")
    if intent.get("schema") != TERMINAL_INTENT_SCHEMA:
        _invalid("terminal_intent_schema_invalid")

    normalized_symbol = str(expected_symbol or "").upper().strip()
    raw_symbol = intent.get("symbol")
    if (
        not normalized_symbol
        or not isinstance(raw_symbol, str)
        or raw_symbol != normalized_symbol
    ):
        _invalid("terminal_intent_symbol_mismatch")
    if intent.get("action") != "lifecycle_drain":
        _invalid("terminal_intent_action_invalid")
    if intent.get("exit_policy") != "use_immutable_run_contract":
        _invalid("terminal_intent_exit_policy_invalid")

    status = intent.get("status")
    if not isinstance(status, str) or status not in TERMINAL_INTENT_STATUSES:
        _invalid("terminal_intent_status_invalid")
    source = intent.get("source")
    if (
        not isinstance(source, str)
        or source != source.strip()
        or not source
        or len(source) > 80
        or any(character.isspace() for character in source)
    ):
        _invalid("terminal_intent_source_invalid")
    intent_id = intent.get("intent_id")
    if (
        not isinstance(intent_id, str)
        or intent_id != intent_id.strip()
        or not intent_id
        or len(intent_id) > 200
        or any(character.isspace() for character in intent_id)
    ):
        _invalid("terminal_intent_id_invalid")

    snapshot = intent.get("run_contract_snapshot")
    if not isinstance(snapshot, dict):
        _invalid("terminal_intent_snapshot_missing")
    try:
        canonical_snapshot = run_contract_snapshot_from_config(snapshot)
        snapshot_contract_id = run_contract_identity_from_config(
            canonical_snapshot
        )
    except (TypeError, ValueError) as exc:
        _invalid("terminal_intent_snapshot_invalid", str(exc))
    if canonical_snapshot != snapshot:
        _invalid("terminal_intent_snapshot_not_canonical")
    if canonical_snapshot.get("symbol") != normalized_symbol:
        _invalid("terminal_intent_snapshot_symbol_mismatch")
    run_contract_id = intent.get("run_contract_id")
    if not isinstance(run_contract_id, str) or not run_contract_id.strip():
        _invalid("terminal_intent_run_contract_missing")
    if run_contract_id != snapshot_contract_id:
        _invalid("terminal_intent_snapshot_digest_mismatch")

    observed = intent.get("observed")
    if not isinstance(observed, dict):
        _invalid("terminal_intent_observed_missing")
    gross_notional = _finite_number(
        observed.get("gross_notional"),
        code="terminal_intent_observed_invalid",
    )
    observed_target = _finite_number(
        observed.get("target"),
        code="terminal_intent_observed_invalid",
    )
    realized_pnl = _finite_number(
        observed.get("realized_pnl"),
        code="terminal_intent_observed_invalid",
    )
    wear_per_10k = _finite_number(
        observed.get("wear_per_10k"),
        code="terminal_intent_observed_invalid",
    )
    trade_count = observed.get("trade_count")
    if (
        isinstance(trade_count, bool)
        or not isinstance(trade_count, int)
        or trade_count < 0
    ):
        _invalid("terminal_intent_observed_invalid")
    if gross_notional < 0 or observed_target < 0:
        _invalid("terminal_intent_observed_invalid")

    snapshot_target_raw = canonical_snapshot.get("max_cumulative_notional")
    snapshot_target = (
        0.0 if snapshot_target_raw is None else float(snapshot_target_raw)
    )
    if not math.isclose(
        observed_target,
        snapshot_target,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        _invalid("terminal_intent_observed_target_mismatch")

    raw_window_start = observed.get("window_start")
    raw_window_end = observed.get("window_end")
    if raw_window_start != canonical_snapshot.get(
        "runtime_guard_stats_start_time"
    ):
        _invalid("terminal_intent_observed_start_mismatch")
    if raw_window_end != canonical_snapshot.get("run_end_time"):
        _invalid("terminal_intent_observed_end_mismatch")
    window_start = _aware_timestamp(
        raw_window_start,
        code="terminal_intent_observed_window_invalid",
    )
    window_end = _aware_timestamp(
        raw_window_end,
        code="terminal_intent_observed_window_invalid",
    )
    query_end = _aware_timestamp(
        observed.get("query_end"),
        code="terminal_intent_observed_window_invalid",
    )
    requested_at = _aware_timestamp(
        intent.get("requested_at"),
        code="terminal_intent_observed_window_invalid",
    )
    if (
        window_start >= window_end
        or not (window_start <= query_end <= window_end)
        or query_end > requested_at
    ):
        _invalid("terminal_intent_observed_window_invalid")

    expected_wear_per_10k = (
        -realized_pnl / gross_notional * 10_000.0
        if gross_notional > 0
        else 0.0
    )
    if not math.isclose(
        wear_per_10k,
        expected_wear_per_10k,
        rel_tol=1e-9,
        abs_tol=1e-9,
    ):
        _invalid("terminal_intent_wear_calculation_mismatch")

    trigger_reason = intent.get("trigger_reason")
    if (
        not isinstance(trigger_reason, str)
        or trigger_reason not in TERMINAL_INTENT_TRIGGER_REASONS
    ):
        _invalid("terminal_intent_trigger_reason_invalid")
    if trigger_reason == "target_reached":
        if observed_target <= 0 or gross_notional < observed_target:
            _invalid("terminal_intent_target_proof_invalid")
    elif trigger_reason == "target_unmet_deadline":
        runtime_primary = observed.get("runtime_guard_primary_reason")
        runtime_reasons = observed.get("runtime_guard_matched_reasons")
        reasons_are_valid = (
            isinstance(runtime_reasons, list)
            and all(
                isinstance(reason, str) and bool(reason.strip())
                for reason in runtime_reasons
            )
        )
        if (
            observed_target <= 0
            or gross_notional >= observed_target
            or query_end != window_end
            or requested_at < window_end
            or runtime_primary != "after_end_window"
            or not reasons_are_valid
            or "after_end_window" not in runtime_reasons
        ):
            _invalid("terminal_intent_deadline_proof_invalid")
    else:
        snapshot_first = canonical_snapshot.get(
            "lifecycle_wear_stop_min_gross_notional"
        )
        snapshot_wear_stop = canonical_snapshot.get(
            "lifecycle_wear_stop_per_10k"
        )
        if snapshot_first is None or snapshot_wear_stop is None:
            _invalid("terminal_intent_wear_not_enabled")
        first = _finite_number(
            observed.get("first"),
            code="terminal_intent_observed_invalid",
        )
        wear_stop = _finite_number(
            observed.get("wear_stop"),
            code="terminal_intent_observed_invalid",
        )
        authoritative_first = float(snapshot_first)
        authoritative_wear_stop = float(snapshot_wear_stop)
        if first < 0 or wear_stop < 0 or gross_notional <= 0:
            _invalid("terminal_intent_observed_invalid")
        if not math.isclose(
            first,
            authoritative_first,
            rel_tol=0.0,
            abs_tol=1e-9,
        ) or not math.isclose(
            wear_stop,
            authoritative_wear_stop,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            _invalid("terminal_intent_wear_threshold_mismatch")
        if gross_notional < authoritative_first:
            _invalid("terminal_intent_wear_arm_proof_invalid")
        if wear_per_10k <= authoritative_wear_stop:
            _invalid("terminal_intent_wear_limit_proof_invalid")

    expected_intent_id = terminal_intent_id(
        symbol=normalized_symbol,
        source=source,
        trigger_reason=trigger_reason,
        run_contract_id=run_contract_id,
    )
    if intent_id != expected_intent_id:
        _invalid("terminal_intent_id_invalid")

    return ValidatedTerminalIntent(
        symbol=normalized_symbol,
        source=source,
        intent_id=intent_id,
        status=status,
        trigger_reason=trigger_reason,
        requested_at=requested_at,
        run_contract_id=run_contract_id,
        run_contract_snapshot=canonical_snapshot,
        gross_notional=gross_notional,
        target=observed_target,
        realized_pnl=realized_pnl,
        wear_per_10k=wear_per_10k,
        trade_count=trade_count,
        window_start=window_start,
        window_end=window_end,
        query_end=query_end,
    )


def terminal_drain_exit_contract_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    captured_at: str,
) -> dict[str, Any]:
    return {
        "policy": snapshot.get("terminal_drain_exit_policy"),
        "absolute_loss_budget": snapshot.get(
            "terminal_drain_absolute_loss_budget"
        ),
        "max_wait_seconds": snapshot.get("terminal_drain_max_wait_seconds"),
        "preserve_reason": snapshot.get("terminal_drain_stop_preserve_reason"),
        "strategy_mode": snapshot.get("strategy_mode"),
        "captured_at": str(captured_at),
        "max_order_notional": snapshot.get("terminal_drain_max_order_notional"),
        "loss_lease_seconds": snapshot.get("terminal_drain_loss_lease_seconds"),
        "order_reprice_seconds": snapshot.get(
            "terminal_drain_order_reprice_seconds"
        ),
        "flat_confirm_cycles": snapshot.get("terminal_drain_flat_confirm_cycles"),
    }


def terminal_drain_owner_integrity_digest(envelope: Mapping[str, Any]) -> str:
    payload = {
        "schema": envelope.get("schema"),
        "decision_id": envelope.get("decision_id"),
        "run_contract_id": envelope.get("run_contract_id"),
        "started_at": envelope.get("started_at"),
        "initial_frozen_long_qty": envelope.get("initial_frozen_long_qty"),
        "initial_frozen_short_qty": envelope.get("initial_frozen_short_qty"),
    }
    if envelope.get("initial_owned_ledger_digest") is not None:
        payload.update(
            {
                "initial_owned_ledger_digest": envelope.get(
                    "initial_owned_ledger_digest"
                ),
                "initial_normal_long_qty": envelope.get(
                    "initial_normal_long_qty"
                ),
                "initial_normal_short_qty": envelope.get(
                    "initial_normal_short_qty"
                ),
                "initial_normal_long_cost_basis": envelope.get(
                    "initial_normal_long_cost_basis"
                ),
                "initial_normal_short_cost_basis": envelope.get(
                    "initial_normal_short_cost_basis"
                ),
            }
        )
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return "futures-terminal-owner-v1-" + hashlib.sha256(
        encoded.encode("utf-8")
    ).hexdigest()[:24]


def terminal_drain_runtime_integrity_digest(envelope: Mapping[str, Any]) -> str:
    payload = {
        str(key): value
        for key, value in envelope.items()
        if key != "runtime_integrity_digest"
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return "futures-terminal-runtime-v1-" + hashlib.sha256(
        encoded.encode("utf-8")
    ).hexdigest()[:24]


def _nonnegative_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(parsed):
        return 0.0
    return max(parsed, 0.0)


def _frozen_inventory_qtys(state: Mapping[str, Any]) -> tuple[float, float]:
    ledger = state.get("best_quote_frozen_inventory")
    if not isinstance(ledger, Mapping):
        return 0.0, 0.0
    return (
        _nonnegative_float(ledger.get("long_qty")),
        _nonnegative_float(ledger.get("short_qty")),
    )


def terminal_drain_runtime_owner_is_integral(
    envelope: Mapping[str, Any],
    *,
    symbol: str,
    loop_state: Mapping[str, Any] | None = None,
) -> bool:
    """Validate an active or completed runtime owner without side effects."""

    try:
        if envelope.get("schema") != TERMINAL_DRAIN_SCHEMA:
            return False
        snapshot = envelope.get("run_contract_snapshot")
        if not isinstance(snapshot, dict):
            return False
        canonical = run_contract_snapshot_from_config(snapshot)
        if canonical != snapshot or canonical.get("symbol") != symbol:
            return False
        contract_id = run_contract_identity_from_config(canonical)
        decision_id = f"{symbol}|{contract_id}"
        if envelope.get("run_contract_id") != contract_id:
            return False
        if envelope.get("decision_id") != decision_id:
            return False
        started_at = envelope.get("started_at")
        _aware_timestamp(started_at, code="terminal_runtime_owner_started_at_invalid")
        if envelope.get("exit_contract") != terminal_drain_exit_contract_from_snapshot(
            canonical,
            captured_at=str(started_at or ""),
        ):
            return False
        if envelope.get("owner_integrity_digest") != (
            terminal_drain_owner_integrity_digest(envelope)
        ):
            return False
        if envelope.get("runtime_integrity_digest") != (
            terminal_drain_runtime_integrity_digest(envelope)
        ):
            return False
        raw_drain_state = envelope.get("drain_state")
        if not isinstance(raw_drain_state, dict):
            return False
        drain_state = terminal_drain_state_from_dict(raw_drain_state)
        if drain_state.symbol != symbol or drain_state.decision_id != decision_id:
            return False
        initial_frozen_long = float(envelope.get("initial_frozen_long_qty"))
        initial_frozen_short = float(envelope.get("initial_frozen_short_qty"))
        if (
            not math.isfinite(initial_frozen_long)
            or not math.isfinite(initial_frozen_short)
            or initial_frozen_long < 0
            or initial_frozen_short < 0
        ):
            return False
        if loop_state is not None:
            durable_long, durable_short = _frozen_inventory_qtys(loop_state)
            if (
                abs(initial_frozen_long - durable_long) > 1e-12
                or abs(initial_frozen_short - durable_short) > 1e-12
            ):
                return False
        exit_status = str(envelope.get("exit_status") or "")
        if exit_status in {"exiting", "exit_blocked"}:
            return drain_state.stage not in {
                DrainStage.STOPPED_CLEAN,
                DrainStage.STOPPED_PRESERVED,
            }
        if exit_status == "stopped_clean":
            return drain_state.stage is DrainStage.STOPPED_CLEAN
        if exit_status == "stopped_preserved":
            return drain_state.stage is DrainStage.STOPPED_PRESERVED
        return False
    except (TerminalIntentValidationError, TypeError, ValueError):
        return False


def terminal_drain_completed_owner_is_integral(
    envelope: Mapping[str, Any],
    *,
    symbol: str,
    loop_state: Mapping[str, Any] | None = None,
) -> bool:
    return bool(
        str(envelope.get("exit_status") or "")
        in TERMINAL_DRAIN_COMPLETED_EXIT_STATUSES
        and terminal_drain_runtime_owner_is_integral(
            envelope,
            symbol=symbol,
            loop_state=loop_state,
        )
    )
