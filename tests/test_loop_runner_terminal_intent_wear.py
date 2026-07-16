from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone

import pytest

from grid_optimizer.futures_run_lifecycle import (
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.futures_terminal_ownership import terminal_intent_id
from grid_optimizer.loop_runner import _build_parser, _terminal_intent_runtime_args


REQUESTED_AT = datetime(2026, 7, 16, 1, 30, tzinfo=timezone.utc)


def test_loop_runner_parser_defaults_wear_exit_off_and_accepts_explicit_terms() -> None:
    defaults = _build_parser().parse_args([])
    assert defaults.lifecycle_wear_stop_per_10k is None
    assert defaults.lifecycle_wear_stop_min_gross_notional is None

    configured = _build_parser().parse_args(
        [
            "--lifecycle-wear-stop-per-10k",
            "2",
            "--lifecycle-wear-stop-min-gross-notional",
            "75000",
        ]
    )
    assert configured.lifecycle_wear_stop_per_10k == pytest.approx(2.0)
    assert configured.lifecycle_wear_stop_min_gross_notional == pytest.approx(
        75_000.0
    )


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        symbol="BCHUSDT",
        strategy_profile="bch-volume-v1",
        strategy_mode="hedge_best_quote_maker_volume_v1",
        per_order_notional=20.0,
        run_start_time="2026-07-16T00:00:00+00:00",
        runtime_guard_stats_start_time="2026-07-16T00:00:00+00:00",
        run_end_time="2026-07-17T00:00:00+00:00",
        max_cumulative_notional=100_000.0,
        terminal_drain_exit_policy="drain_then_preserve",
        terminal_drain_absolute_loss_budget=2.0,
        terminal_drain_max_wait_seconds=600.0,
        terminal_drain_stop_preserve_reason=None,
        lifecycle_wear_stop_per_10k=2.0,
        lifecycle_wear_stop_min_gross_notional=75_000.0,
    )


def _wear_intent() -> dict[str, object]:
    args = _args()
    snapshot = run_contract_snapshot_from_config(vars(args))
    contract_id = run_contract_identity_from_config(snapshot)
    return {
        "schema": "futures_lifecycle_intent_v2",
        "intent_id": terminal_intent_id(
            symbol="BCHUSDT",
            source="competition_target_gate",
            trigger_reason="wear_limit_breached",
            run_contract_id=contract_id,
        ),
        "symbol": "BCHUSDT",
        "source": "competition_target_gate",
        "action": "lifecycle_drain",
        "trigger_reason": "wear_limit_breached",
        "requested_at": REQUESTED_AT.isoformat(),
        "status": "pending",
        "exit_policy": "use_immutable_run_contract",
        "run_contract_id": contract_id,
        "run_contract_snapshot": snapshot,
        "observed": {
            "gross_notional": 80_000.0,
            "target": 100_000.0,
            "realized_pnl": -24.0,
            "wear_per_10k": 3.0,
            "trade_count": 10,
            "first": 75_000.0,
            "wear_stop": 2.0,
            "window_start": "2026-07-16T00:00:00+00:00",
            "window_end": "2026-07-17T00:00:00+00:00",
            "query_end": "2026-07-16T01:30:00+00:00",
        },
    }


def test_wear_terminal_intent_accepts_consistent_breach_proof() -> None:
    effective_args, error = _terminal_intent_runtime_args(
        args=_args(),
        terminal_intent=_wear_intent(),
    )

    assert error is None
    assert effective_args.symbol == "BCHUSDT"
    assert effective_args.max_cumulative_notional == pytest.approx(100_000.0)


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("first", 80_001.0, "terminal_intent_wear_threshold_mismatch"),
        ("wear_stop", 3.0, "terminal_intent_wear_threshold_mismatch"),
        ("gross_notional", 90_000.0, "terminal_intent_wear_calculation_mismatch"),
        ("realized_pnl", -20.0, "terminal_intent_wear_calculation_mismatch"),
        ("wear_per_10k", 2.5, "terminal_intent_wear_calculation_mismatch"),
    ],
)
def test_wear_terminal_intent_rejects_inconsistent_proof(
    field: str,
    value: float,
    expected_error: str,
) -> None:
    intent = copy.deepcopy(_wear_intent())
    intent["observed"][field] = value  # type: ignore[index]

    _effective_args, error = _terminal_intent_runtime_args(
        args=_args(),
        terminal_intent=intent,
    )

    assert error == expected_error


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("first", 74_999.0),
        ("wear_stop", 1.9),
    ],
)
def test_wear_terminal_intent_rejects_thresholds_not_bound_to_snapshot(
    field: str,
    value: float,
) -> None:
    intent = copy.deepcopy(_wear_intent())
    intent["observed"][field] = value  # type: ignore[index]

    _effective_args, error = _terminal_intent_runtime_args(
        args=_args(),
        terminal_intent=intent,
    )

    assert error == "terminal_intent_wear_threshold_mismatch"


def test_wear_terminal_intent_is_rejected_when_snapshot_disables_wear_exit() -> None:
    args = _args()
    args.lifecycle_wear_stop_per_10k = None
    args.lifecycle_wear_stop_min_gross_notional = None
    snapshot = run_contract_snapshot_from_config(vars(args))
    intent = _wear_intent()
    intent["run_contract_snapshot"] = snapshot
    intent["run_contract_id"] = run_contract_identity_from_config(snapshot)

    _effective_args, error = _terminal_intent_runtime_args(
        args=args,
        terminal_intent=intent,
    )

    assert error == "terminal_intent_wear_not_enabled"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("first", float("inf")),
        ("wear_stop", float("nan")),
        ("gross_notional", float("inf")),
        ("realized_pnl", float("nan")),
        ("wear_per_10k", float("inf")),
    ],
)
def test_wear_terminal_intent_rejects_non_finite_evidence(
    field: str,
    value: float,
) -> None:
    intent = copy.deepcopy(_wear_intent())
    intent["observed"][field] = value  # type: ignore[index]

    _effective_args, error = _terminal_intent_runtime_args(
        args=_args(),
        terminal_intent=intent,
    )

    assert error == "terminal_intent_observed_invalid"
