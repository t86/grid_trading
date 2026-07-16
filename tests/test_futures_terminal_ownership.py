from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest

from grid_optimizer.futures_run_lifecycle import (
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.futures_terminal_ownership import (
    TerminalIntentValidationError,
    validate_terminal_intent,
)


def _snapshot(*, wear_enabled: bool = False) -> dict[str, object]:
    return run_contract_snapshot_from_config(
        {
            "symbol": "BCHUSDT",
            "strategy_profile": "bch-volume-v1",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "per_order_notional": 20.0,
            "run_start_time": "2026-07-16T00:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
            "run_end_time": "2026-07-17T00:00:00+00:00",
            "max_cumulative_notional": 20_000.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 2.0,
            "terminal_drain_max_wait_seconds": 600.0,
            "lifecycle_wear_stop_per_10k": 2.0 if wear_enabled else None,
            "lifecycle_wear_stop_min_gross_notional": (
                75_000.0 if wear_enabled else None
            ),
        }
    )


def _intent(
    *,
    trigger_reason: str = "target_reached",
    status: str = "pending",
) -> dict[str, object]:
    snapshot = _snapshot(wear_enabled=trigger_reason == "wear_limit_breached")
    contract_id = run_contract_identity_from_config(snapshot)
    source = "competition_target_gate"
    identity = json.dumps(
        {
            "symbol": "BCHUSDT",
            "source": source,
            "trigger_reason": trigger_reason,
            "run_contract_id": contract_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    intent_id = (
        f"BCHUSDT-{source}-{trigger_reason}-"
        f"{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:16]}"
    )
    requested_at = "2026-07-16T01:00:00+00:00"
    observed: dict[str, object] = {
        "gross_notional": 20_001.0,
        "target": 20_000.0,
        "realized_pnl": 0.0,
        "wear_per_10k": 0.0,
        "trade_count": 10,
        "window_start": snapshot["runtime_guard_stats_start_time"],
        "window_end": snapshot["run_end_time"],
        "query_end": requested_at,
    }
    if trigger_reason == "target_unmet_deadline":
        requested_at = "2026-07-17T00:00:01+00:00"
        observed.update(
            {
                "gross_notional": 19_999.0,
                "query_end": snapshot["run_end_time"],
                "runtime_guard_primary_reason": "after_end_window",
                "runtime_guard_matched_reasons": ["after_end_window"],
            }
        )
    elif trigger_reason == "wear_limit_breached":
        observed.update(
            {
                "gross_notional": 80_000.0,
                "realized_pnl": -20.0,
                "wear_per_10k": 2.5,
                "first": 75_000.0,
                "wear_stop": 2.0,
            }
        )
    return {
        "schema": "futures_lifecycle_intent_v2",
        "intent_id": intent_id,
        "symbol": "BCHUSDT",
        "source": source,
        "action": "lifecycle_drain",
        "trigger_reason": trigger_reason,
        "requested_at": requested_at,
        "status": status,
        "exit_policy": "use_immutable_run_contract",
        "run_contract_id": contract_id,
        "run_contract_snapshot": snapshot,
        "observed": observed,
    }


@pytest.mark.parametrize(
    ("trigger_reason", "status"),
    (
        ("target_reached", "pending"),
        ("target_unmet_deadline", "accepted"),
        ("wear_limit_breached", "executing"),
        ("target_reached", "stopped_clean"),
    ),
)
def test_shared_terminal_intent_validator_accepts_complete_proof(
    trigger_reason: str,
    status: str,
) -> None:
    intent = _intent(trigger_reason=trigger_reason, status=status)

    validated = validate_terminal_intent(intent, expected_symbol="BCHUSDT")

    assert validated.status == status
    assert validated.trigger_reason == trigger_reason
    assert validated.run_contract_id == intent["run_contract_id"]
    assert validated.run_contract_snapshot == intent["run_contract_snapshot"]


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    (
        (lambda value: value.pop("observed"), "terminal_intent_observed_missing"),
        (
            lambda value: value["observed"].pop("gross_notional"),
            "terminal_intent_observed_invalid",
        ),
        (
            lambda value: value["observed"].update(target=19_999.0),
            "terminal_intent_observed_target_mismatch",
        ),
        (
            lambda value: value["observed"].update(realized_pnl=-1.0),
            "terminal_intent_wear_calculation_mismatch",
        ),
        (
            lambda value: value["observed"].update(trade_count=1.5),
            "terminal_intent_observed_invalid",
        ),
        (
            lambda value: value.update(requested_at="2026-07-16T01:00:00"),
            "terminal_intent_observed_window_invalid",
        ),
        (
            lambda value: value["observed"].update(
                query_end="2026-07-15T23:59:59+00:00"
            ),
            "terminal_intent_observed_window_invalid",
        ),
        (
            lambda value: value.update(intent_id="BCHUSDT-forged"),
            "terminal_intent_id_invalid",
        ),
    ),
)
def test_shared_terminal_intent_validator_rejects_missing_or_forged_common_proof(
    mutation,
    expected_code: str,
) -> None:
    intent = _intent()
    mutation(intent)

    with pytest.raises(TerminalIntentValidationError) as exc_info:
        validate_terminal_intent(intent, expected_symbol="BCHUSDT")

    assert exc_info.value.code == expected_code


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    (
        (
            lambda value: value["observed"].update(gross_notional=20_000.0),
            "terminal_intent_deadline_proof_invalid",
        ),
        (
            lambda value: value["observed"].update(
                query_end="2026-07-16T23:59:59+00:00"
            ),
            "terminal_intent_deadline_proof_invalid",
        ),
        (
            lambda value: value["observed"].update(
                runtime_guard_primary_reason="max_actual_net_notional_hit"
            ),
            "terminal_intent_deadline_proof_invalid",
        ),
        (
            lambda value: value["observed"].update(
                runtime_guard_matched_reasons=[]
            ),
            "terminal_intent_deadline_proof_invalid",
        ),
        (
            lambda value: value.update(
                requested_at="2026-07-16T23:59:59+00:00"
            ),
            "terminal_intent_observed_window_invalid",
        ),
    ),
)
def test_shared_terminal_intent_validator_rejects_forged_deadline_proof(
    mutation,
    expected_code: str,
) -> None:
    intent = _intent(trigger_reason="target_unmet_deadline")
    mutation(intent)

    with pytest.raises(TerminalIntentValidationError) as exc_info:
        validate_terminal_intent(intent, expected_symbol="BCHUSDT")

    assert exc_info.value.code == expected_code


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    (
        (
            lambda value: value["observed"].update(first=74_999.0),
            "terminal_intent_wear_threshold_mismatch",
        ),
        (
            lambda value: value["observed"].update(wear_stop=3.0),
            "terminal_intent_wear_threshold_mismatch",
        ),
        (
            lambda value: value["observed"].update(
                gross_notional=74_999.0,
                realized_pnl=-18.74975,
            ),
            "terminal_intent_wear_arm_proof_invalid",
        ),
        (
            lambda value: value["observed"].update(
                wear_per_10k=2.0,
                realized_pnl=-16.0,
            ),
            "terminal_intent_wear_limit_proof_invalid",
        ),
        (
            lambda value: value["observed"].update(realized_pnl=-10.0),
            "terminal_intent_wear_calculation_mismatch",
        ),
    ),
)
def test_shared_terminal_intent_validator_rejects_forged_wear_proof(
    mutation,
    expected_code: str,
) -> None:
    intent = _intent(trigger_reason="wear_limit_breached")
    mutation(intent)

    with pytest.raises(TerminalIntentValidationError) as exc_info:
        validate_terminal_intent(intent, expected_symbol="BCHUSDT")

    assert exc_info.value.code == expected_code


def test_shared_terminal_intent_validator_does_not_mutate_payload() -> None:
    intent = _intent(trigger_reason="wear_limit_breached")
    before = deepcopy(intent)

    validate_terminal_intent(intent, expected_symbol="BCHUSDT")

    assert intent == before
