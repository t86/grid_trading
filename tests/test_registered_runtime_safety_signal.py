from __future__ import annotations

import argparse
import json
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer import bq_volume_recovery_guard
from grid_optimizer.futures_recovery_coordinator import ActionId
from grid_optimizer.futures_recovery_store import JsonRecoveryStore
from grid_optimizer.futures_recovery_runtime_signal import (
    RUNTIME_SAFETY_SIGNAL_KEY,
)
from grid_optimizer.futures_run_lifecycle import run_contract_identity_from_config
from grid_optimizer.loop_runner import (
    _apply_registered_runtime_safety_signal_to_plan,
    _apply_registered_runtime_safety_signal_to_actions,
    _guard_registered_runtime_safety_order_before_submit,
    _guard_cancel_order_before_submit,
    _maybe_handle_runtime_guard,
    _protective_entry_stop,
    _replace_registered_runtime_safety_source,
    _runtime_safety_signal_ttl,
    _validate_registered_runtime_safety_ttl,
)
from grid_optimizer.runtime_guards import RuntimeGuardResult
from grid_optimizer.submit_plan import apply_actual_net_exposure_decision_to_actions


NOW = datetime(2026, 7, 16, 1, tzinfo=timezone.utc)


def _registered_args(tmpdir: str) -> tuple[argparse.Namespace, object]:
    output_dir = Path(tmpdir)
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    state_path = output_dir / "bchusdt_loop_state.json"
    args = argparse.Namespace(
        symbol="BCHUSDT",
        strategy_profile="bch-test-v1",
        strategy_mode="hedge_best_quote_maker_volume_v1",
        per_order_notional=20.0,
        run_start_time=None,
        runtime_guard_stats_start_time=None,
        run_end_time=None,
        rolling_hourly_loss_limit=8.0,
        rolling_hourly_loss_per_10k_limit=None,
        rolling_hourly_loss_per_10k_min_notional=None,
        max_cumulative_notional=None,
        max_actual_net_notional=None,
        max_synthetic_drift_notional=None,
        max_unrealized_loss=None,
        terminal_drain_exit_policy=None,
        terminal_drain_absolute_loss_budget=None,
        terminal_drain_max_wait_seconds=None,
        terminal_drain_stop_preserve_reason=None,
        terminal_drain_max_order_notional=20.0,
        terminal_drain_loss_lease_seconds=300.0,
        terminal_drain_order_reprice_seconds=120.0,
        terminal_drain_flat_confirm_cycles=2,
        lifecycle_wear_stop_per_10k=None,
        lifecycle_wear_stop_min_gross_notional=None,
        runtime_guard_loss_recovery_enabled=True,
        state_path=str(state_path),
        plan_json=str(output_dir / "bchusdt_loop_latest_plan.json"),
        recv_window=5000,
        recovery_control_path=str(control_path),
        recovery_generation=None,
    )
    registered = JsonRecoveryStore(control_path).register_symbol(
        "BCHUSDT",
        {
            **vars(args),
            "best_quote_maker_volume_allow_loss_reduce_only": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "hard_loss_forced_reduce_enabled": False,
            "volatility_entry_pause_enabled": True,
        },
        now=NOW - timedelta(minutes=1),
    )
    args.recovery_generation = registered.generation
    return args, registered


def _normal_reports(output_dir: Path, state: object, now: datetime) -> None:
    gate = {
        "managed": True,
        "ready": True,
        "reason": None,
        "symbol": state.symbol,
        "generation": state.generation,
        "decision_id": state.decision_id,
        "profile_digest": state.desired_profile.digest,
        "active_action": "noop",
        "side": None,
        "order_role": None,
        "ledger_class": None,
        "allowed_orders": [],
        "allowed_roles": [],
        "progress_deadline_at": None,
        "hard_expires_at": None,
    }
    (output_dir / "bchusdt_loop_latest_plan.json").write_text(
        json.dumps(
            {
                "generated_at": (now - timedelta(seconds=1)).isoformat(),
                "current_long_notional": 0.0,
                "current_short_notional": 0.0,
                "effective_max_position_notional": 100.0,
                "effective_max_short_position_notional": 100.0,
                "buy_orders": [],
                "sell_orders": [],
                "recovery_profile_gate": {
                    **gate,
                    "dropped_order_count": 0,
                    "dropped_orders": [],
                },
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "bchusdt_loop_latest_submit.json").write_text(
        json.dumps(
            {
                "generated_at": now.isoformat(),
                "submit_generated_at": now.isoformat(),
                "observed_strategy_open_order_state": {"active_order_count": 0},
                "recovery_profile_execution": {
                    "managed": True,
                    "authorized": True,
                    "reason": None,
                    "current_gate": gate,
                    "dropped_place_count": 0,
                    "dropped_cancel_count": 0,
                    "dropped_orders": [],
                },
                "validation": {
                    "actions": {
                        "place_orders": [],
                        "reduce_only_no_loss_guard": {
                            "enabled": True,
                            "dropped_order_count": 0,
                            "dropped_orders": [],
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _frozen_short_request(
    *,
    request_id: str,
    order_id: int | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    order: dict[str, object] = {
        "book": "frozen_bq",
        "role": "frozen_inventory_manual_reduce_short",
        "frozen_inventory_request_id": request_id,
        "side": "BUY",
        "position_side": "SHORT",
        "qty": 0.1,
        "price": 200.0,
        "force_reduce_only": True,
        "execution_type": "maker",
        "time_in_force": "GTX",
        "post_only": True,
        "manual_frozen_inventory_reduce": True,
        "frozen_inventory_authorization_validated": True,
    }
    refs: dict[str, object] = {}
    if order_id is not None:
        client_order_id = f"gx-bchu-frozenin-1-{order_id}"
        order.update(
            {
                "orderId": order_id,
                "clientOrderId": client_order_id,
                "origQty": "0.1",
                "executedQty": "0",
            }
        )
        refs[str(order_id)] = {
            "book": "frozen_bq",
            "role": "frozen_inventory_manual_reduce_short",
            "side": "BUY",
            "position_side": "SHORT",
            "client_order_id": client_order_id,
            "frozen_inventory_request_id": request_id,
        }
    state = {
        "best_quote_frozen_inventory": {
            "long_qty": 0.0,
            "long_lots": [],
            "short_qty": 0.2,
            "short_lots": [{"qty": 0.2, "entry_price": 201.0}],
        },
        "best_quote_frozen_inventory_manual_reduce": {
            "short": {
                "requested": True,
                "request_id": request_id,
                "requested_qty": 0.1,
                "requested_at": NOW.isoformat(),
                "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
            }
        },
        "best_quote_volume_order_refs": refs,
    }
    return order, state


@patch("grid_optimizer.loop_runner._start_futures_flatten_process")
@patch("grid_optimizer.loop_runner.load_live_flatten_snapshot")
@patch("grid_optimizer.loop_runner._cancel_futures_strategy_orders")
@patch("grid_optimizer.loop_runner.load_binance_api_credentials")
@patch("grid_optimizer.loop_runner.evaluate_runtime_guards")
@patch("grid_optimizer.loop_runner._load_futures_runtime_guard_inputs")
def test_registered_runtime_loss_guard_writes_signal_without_acting(
    mock_inputs,
    mock_evaluate,
    mock_credentials,
    mock_cancel,
    mock_flatten_snapshot,
    mock_start_flatten,
) -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        mock_inputs.return_value = (1000.0, [], NOW - timedelta(hours=1))
        mock_evaluate.return_value = RuntimeGuardResult(
            tradable=False,
            stop_triggered=True,
            runtime_status="stopped",
            primary_reason="rolling_hourly_loss_limit_hit",
            matched_reasons=["rolling_hourly_loss_limit_hit"],
            triggered_at=NOW.isoformat(),
            rolling_hourly_loss=9.0,
            rolling_hourly_gross_notional=1000.0,
            rolling_hourly_loss_per_10k=90.0,
            rolling_hourly_loss_per_10k_active=True,
            cumulative_gross_notional=1000.0,
            actual_net_notional_abs=0.0,
            synthetic_drift_notional=0.0,
            unrealized_loss=0.0,
        )

        summary = _maybe_handle_runtime_guard(
            args=args,
            cycle=1,
            cycle_started_at=NOW,
            summary_path=Path(tmpdir) / "summary.jsonl",
        )

        state = json.loads(Path(args.state_path).read_text(encoding="utf-8"))

    assert summary is None
    signal = state["futures_recovery_runtime_safety_signal"]
    assert signal["schema"] == "futures_recovery_runtime_safety_signal_v1"
    assert signal["symbol"] == "BCHUSDT"
    assert signal["generation"] == registered.generation
    assert signal["decision_id"] == registered.decision_id
    assert signal["hard_expires_at"] == (
        NOW + timedelta(seconds=120)
    ).isoformat()
    assert signal["conditions"] == [
        {
            "source": "runtime_guard",
            "action_id": "safety_converge",
            "reason": "rolling_hourly_loss_limit_hit",
            "side": None,
            "observed_at": NOW.isoformat(),
            "hard_expires_at": (
                NOW + timedelta(seconds=120)
            ).isoformat(),
            "details_digest": signal["conditions"][0]["details_digest"],
        }
    ]
    mock_credentials.assert_not_called()
    mock_cancel.assert_not_called()
    mock_flatten_snapshot.assert_not_called()
    mock_start_flatten.assert_not_called()


def test_registered_runtime_signal_action_gate_allows_only_normal_gtx_reduce() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    "futures_recovery_runtime_safety_signal": {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (
                            NOW + timedelta(seconds=120)
                        ).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "safety_converge",
                                "reason": "max_unrealized_loss_hit",
                                "side": None,
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (
                                    NOW + timedelta(seconds=120)
                                ).isoformat(),
                                "details_digest": "sha256:" + "1" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        actions, report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [{"orderId": 1}],
                "place_orders": [
                    {
                        "role": "best_quote_entry_long",
                        "side": "BUY",
                        "execution_type": "post_only",
                    },
                    {
                        "role": "best_quote_reduce_long",
                        "side": "SELL",
                        "execution_type": "post_only",
                    },
                    {
                        "role": "best_quote_reduce_short",
                        "side": "BUY",
                        "execution_type": "aggressive",
                    },
                    {
                        "role": "best_quote_reduce_short",
                        "side": "SELL",
                        "execution_type": "post_only",
                    },
                ],
                "cancel_count": 1,
                "place_count": 4,
            },
            now=NOW + timedelta(seconds=1),
        )

    assert report["active"] is True
    assert actions["cancel_orders"] == []
    assert actions["cancel_count"] == 0
    assert actions["place_orders"] == [
        {
            "role": "best_quote_reduce_long",
            "side": "SELL",
            "execution_type": "post_only",
        }
    ]
    assert actions["place_count"] == 1


def test_cancel_submit_guard_rejects_managed_generation_change() -> None:
    stale_gate = SimpleNamespace(
        managed=True,
        ready=False,
        reason="recovery_generation_mismatch",
        active_action=ActionId.INVENTORY_RECOVER,
        matches=lambda _expected: False,
    )
    with (
        patch(
            "grid_optimizer.loop_runner._load_recovery_order_profile_gate",
            return_value=stale_gate,
        ),
        patch(
            "grid_optimizer.loop_runner._registered_runtime_safety_signal_gate",
            return_value=(None, {"active": False, "reason": None}),
        ),
    ):
        guarded, reason = _guard_cancel_order_before_submit(
            args=argparse.Namespace(),
            order={"orderId": 7, "side": "BUY"},
            expected_gate={"managed": True, "generation": 1},
            now=NOW,
        )

    assert guarded is None
    assert reason["reason"] == "recovery_cancel_not_authorized"
    assert reason["gate_reason"] == "recovery_generation_mismatch"


def test_cancel_submit_guard_preserves_authorized_frozen_order_outside_ordinary_owner() -> None:
    stale_gate = SimpleNamespace(
        managed=True,
        ready=False,
        reason="recovery_generation_mismatch",
        active_action=ActionId.INVENTORY_RECOVER,
        matches=lambda _expected: False,
    )
    with TemporaryDirectory() as tmpdir:
        frozen_order, frozen_state = _frozen_short_request(
            request_id="frozen-cancel-1",
            order_id=71,
        )
        state_path = Path(tmpdir) / "state.json"
        state_path.write_text(json.dumps(frozen_state), encoding="utf-8")
        with (
            patch(
                "grid_optimizer.loop_runner._load_recovery_order_profile_gate",
                return_value=stale_gate,
            ),
            patch(
                "grid_optimizer.loop_runner._registered_runtime_safety_signal_gate",
                return_value=(None, {"active": False, "reason": None}),
            ),
        ):
            guarded, reason = _guard_cancel_order_before_submit(
                args=argparse.Namespace(state_path=str(state_path)),
                order=frozen_order,
                expected_gate={"managed": True, "generation": 1},
                now=NOW,
            )

    assert guarded is not None
    assert guarded["orderId"] == 71
    assert guarded["frozen_inventory_authorization_validated"] is True
    assert reason is None


def test_cancel_submit_guard_rechecks_directional_runtime_signal() -> None:
    unmanaged_gate = SimpleNamespace(managed=False)
    signal = SimpleNamespace(
        conditions=(SimpleNamespace(side=SimpleNamespace(value="SELL")),),
    )
    with (
        patch(
            "grid_optimizer.loop_runner._load_recovery_order_profile_gate",
            return_value=unmanaged_gate,
        ),
        patch(
            "grid_optimizer.loop_runner._registered_runtime_safety_signal_gate",
            return_value=(
                signal,
                {"active": True, "reason": "runtime_safety_signal_active"},
            ),
        ),
    ):
        blocked, blocked_reason = _guard_cancel_order_before_submit(
            args=argparse.Namespace(),
            order={"orderId": 8, "side": "SELL"},
            expected_gate=None,
            now=NOW,
        )
        allowed, allowed_reason = _guard_cancel_order_before_submit(
            args=argparse.Namespace(),
            order={"orderId": 9, "side": "BUY"},
            expected_gate=None,
            now=NOW,
        )
        refresh, refresh_reason = _guard_cancel_order_before_submit(
            args=argparse.Namespace(),
            order={"orderId": 10, "side": "SELL"},
            expected_gate=None,
            actual_net_decision={"action": "cancel_net_decrease_refresh"},
            now=NOW,
        )

    assert blocked is None
    assert blocked_reason["reason"] == "runtime_safety_signal_blocks_cancel"
    assert allowed == {"orderId": 9, "side": "BUY"}
    assert allowed_reason is None
    assert refresh == {"orderId": 10, "side": "SELL"}
    assert refresh_reason is None


def test_position_cap_signal_preserves_owned_reduce_refresh_for_final_arbiter() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    RUNTIME_SAFETY_SIGNAL_KEY: {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "inventory_recover",
                                "reason": "max_actual_net_notional_hit",
                                "side": "SELL",
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                                "details_digest": "sha256:" + "7" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        old_reduce = {
            "orderId": 88,
            "clientOrderId": "gx-bchu-bestquot-88",
            "side": "SELL",
            "positionSide": "LONG",
            "price": "210",
            "origQty": "0.05",
            "executedQty": "0",
        }
        candidates, _report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [old_reduce],
                "place_orders": [
                    {
                        "side": "SELL",
                        "position_side": "LONG",
                        "price": 200.0,
                        "qty": 0.05,
                        "notional": 10.0,
                        "role": "best_quote_reduce_long",
                        "time_in_force": "GTX",
                    }
                ],
                "cancel_count": 1,
                "place_count": 1,
                "recovery_profile_gate": {
                    "managed": True,
                    "authorized": True,
                },
            },
            now=NOW + timedelta(seconds=1),
        )
        decision = apply_actual_net_exposure_decision_to_actions(
            actions=candidates,
            current_actual_net_qty=0.64,
            valuation_price=200.0,
            current_open_orders=[old_reduce],
            owned_open_orders=[old_reduce],
            max_actual_net_notional=120.0,
        )

    assert decision["actual_net_exposure_decision"]["action"] == (
        "cancel_net_decrease_refresh"
    )
    assert [row["orderId"] for row in decision["cancel_orders"]] == [88]
    assert decision["place_orders"] == []


def test_position_cap_signal_preserves_owned_cancel_candidates_for_final_decision() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    RUNTIME_SAFETY_SIGNAL_KEY: {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "inventory_recover",
                                "reason": "max_actual_net_notional_hit",
                                "side": "SELL",
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                                "details_digest": "sha256:" + "2" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        actions, report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [
                    {"orderId": 1, "side": "BUY", "role": "best_quote_entry_long"},
                    {"orderId": 2, "side": "SELL", "role": "best_quote_reduce_long"},
                ],
                "place_orders": [
                    {
                        "role": "best_quote_reduce_long",
                        "side": "SELL",
                        "execution_type": "post_only",
                        "time_in_force": "GTX",
                    },
                    {
                        "role": "best_quote_reduce_short",
                        "side": "BUY",
                        "execution_type": "post_only",
                        "time_in_force": "GTX",
                    },
                ],
                "cancel_count": 2,
                "place_count": 2,
            },
            now=NOW + timedelta(seconds=1),
        )

        assert report["active"] is True
        assert report["allowed_reduce_side"] == "SELL"
        # The runtime signal is a candidate fence.  The final actual-net
        # arbiter chooses risk BUY cancellation versus stale SELL refresh.
        assert [row["orderId"] for row in actions["cancel_orders"]] == [1, 2]
        assert actions["place_orders"] == []
        guarded, reason = _guard_registered_runtime_safety_order_before_submit(
            args=args,
            order={
                "role": "best_quote_reduce_short",
                "side": "BUY",
                "execution_type": "post_only",
                "time_in_force": "GTX",
            },
            now=NOW + timedelta(seconds=1),
        )
        assert guarded is None
        assert reason["reason"] == "runtime_safety_signal_blocks_risk_increase"


def test_position_cap_signal_keeps_only_owned_reduce_role() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    RUNTIME_SAFETY_SIGNAL_KEY: {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "inventory_recover",
                                "reason": "max_actual_net_notional_hit",
                                "side": "SELL",
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                                "details_digest": "sha256:" + "6" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        actions, _report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [],
                "place_orders": [
                    {
                        "role": "best_quote_reduce_long",
                        "side": "SELL",
                        "execution_type": "post_only",
                        "time_in_force": "GTX",
                    },
                    {
                        "role": "best_quote_entry_short",
                        "side": "SELL",
                        "execution_type": "post_only",
                        "time_in_force": "GTX",
                    },
                    {
                        "role": "best_quote_reduce_short",
                        "side": "BUY",
                        "execution_type": "post_only",
                        "time_in_force": "GTX",
                    },
                ],
                "cancel_count": 0,
                "place_count": 3,
            },
            now=NOW + timedelta(seconds=1),
        )

    assert [row["role"] for row in actions["place_orders"]] == [
        "best_quote_reduce_long",
    ]


def test_position_cap_signal_and_submit_fences_do_not_own_frozen_lane() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        frozen_order, frozen_state = _frozen_short_request(
            request_id="frozen-request-91",
            order_id=91,
        )
        Path(args.state_path).write_text(
            json.dumps(
                {
                    **frozen_state,
                    RUNTIME_SAFETY_SIGNAL_KEY: {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "inventory_recover",
                                "reason": "max_actual_net_notional_hit",
                                "side": "SELL",
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (NOW + timedelta(seconds=120)).isoformat(),
                                "details_digest": "sha256:" + "8" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        actions, _report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [],
                "place_orders": [frozen_order],
                "cancel_count": 0,
                "place_count": 1,
            },
            now=NOW + timedelta(seconds=1),
        )
        plan = {
            "bootstrap_orders": [],
            "buy_orders": [dict(frozen_order)],
            "sell_orders": [],
        }
        _apply_registered_runtime_safety_signal_to_plan(
            args=args,
            plan=plan,
            now=NOW + timedelta(seconds=1),
        )
        guarded, reason = _guard_registered_runtime_safety_order_before_submit(
            args=args,
            order=frozen_order,
            now=NOW + timedelta(seconds=1),
        )
        cancel, cancel_reason = _guard_cancel_order_before_submit(
            args=args,
            order=frozen_order,
            expected_gate=None,
            now=NOW + timedelta(seconds=1),
        )

    assert [row["orderId"] for row in actions["place_orders"]] == [91]
    assert [row["orderId"] for row in plan["buy_orders"]] == [91]
    assert guarded is not None
    assert guarded["frozen_inventory_authorization_validated"] is True
    assert reason is None
    assert cancel is not None
    assert cancel["orderId"] == 91
    assert cancel_reason is None


def test_runtime_safety_submit_fence_rejects_frozen_order_without_request_identity() -> None:
    signal = SimpleNamespace(
        conditions=(SimpleNamespace(side=SimpleNamespace(value="SELL")),),
    )
    unauthorized_frozen = {
        "book": "frozen_bq",
        "role": "frozen_inventory_pair_release_short",
        "side": "BUY",
        "position_side": "SHORT",
        "execution_type": "post_only",
        "time_in_force": "GTX",
        "frozen_inventory_pair_release": True,
    }
    with patch(
        "grid_optimizer.loop_runner._registered_runtime_safety_signal_gate",
        return_value=(
            signal,
            {"active": True, "reason": "runtime_safety_signal_active"},
        ),
    ):
        guarded, reason = _guard_registered_runtime_safety_order_before_submit(
            args=argparse.Namespace(),
            order=unauthorized_frozen,
            now=NOW,
        )

    assert guarded is None
    assert reason is not None
    assert reason["reason"] == "frozen_inventory_state_unavailable"


def test_expired_signal_has_absolute_deadline_and_releases_local_entry_gate() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    "futures_recovery_runtime_safety_signal": {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (
                            NOW + timedelta(seconds=120)
                        ).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "safety_converge",
                                "reason": "max_unrealized_loss_hit",
                                "side": None,
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (
                                    NOW + timedelta(seconds=120)
                                ).isoformat(),
                                "details_digest": "sha256:" + "3" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        original = {
            "cancel_orders": [{"orderId": 1}],
            "place_orders": [
                {
                    "role": "best_quote_entry_long",
                    "side": "BUY",
                    "execution_type": "post_only",
                }
            ],
            "cancel_count": 1,
            "place_count": 1,
        }
        actions, report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions=original,
            now=NOW + timedelta(seconds=121),
        )

    assert report["active"] is False
    assert report["reason"] == "runtime_safety_signal_stale"
    assert actions["cancel_orders"] == original["cancel_orders"]
    assert actions["place_orders"] == original["place_orders"]


def test_tampered_signal_above_global_max_ttl_is_rejected_fail_closed() -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        forged_expiry = NOW + timedelta(days=3650)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    RUNTIME_SAFETY_SIGNAL_KEY: {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": forged_expiry.isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "safety_converge",
                                "reason": "max_unrealized_loss_hit",
                                "side": None,
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": forged_expiry.isoformat(),
                                "details_digest": "sha256:" + "5" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        actions, report = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [],
                "place_orders": [
                    {
                        "role": "best_quote_entry_long",
                        "side": "BUY",
                        "execution_type": "post_only",
                    },
                    {
                        "role": "best_quote_reduce_short",
                        "side": "BUY",
                        "execution_type": "post_only",
                    }
                ],
                "cancel_count": 0,
                "place_count": 2,
            },
            now=NOW + timedelta(seconds=1),
        )

    assert report["active"] is True
    assert report["reason"] == "runtime_safety_signal_invalid"
    assert actions["place_count"] == 0


def test_configured_cycle_gap_above_global_max_ttl_is_rejected() -> None:
    args = argparse.Namespace(sleep_seconds=900.0, cycle_jitter_seconds=0.0)

    with pytest.raises(ValueError, match="exceeds maximum"):
        _runtime_safety_signal_ttl(args)


def test_unregistered_startup_keeps_legacy_sleep_above_signal_max() -> None:
    args = argparse.Namespace(
        symbol="LEGACYUSDT",
        recovery_control_path=None,
        recovery_generation=None,
        sleep_seconds=900.0,
        cycle_jitter_seconds=0.0,
    )

    assert _validate_registered_runtime_safety_ttl(args, now=NOW) is None


def test_registered_startup_rejects_sleep_above_signal_max() -> None:
    with TemporaryDirectory() as tmpdir:
        args, _registered = _registered_args(tmpdir)
        args.sleep_seconds = 900.0
        args.cycle_jitter_seconds = 0.0

        with pytest.raises(ValueError, match="exceeds maximum"):
            _validate_registered_runtime_safety_ttl(args, now=NOW)


@patch("grid_optimizer.loop_runner._cancel_futures_strategy_entry_orders")
def test_registered_protective_stop_writes_runner_signal_without_cancel_or_raise(
    mock_cancel_entries,
) -> None:
    with TemporaryDirectory() as tmpdir:
        args, registered = _registered_args(tmpdir)
        state: dict[str, object] = {}

        result = _protective_entry_stop(
            args=args,
            symbol="BCHUSDT",
            strategy_mode=args.strategy_mode,
            api_key="unused",
            api_secret="unused",
            reason="reconcile_protective_stop",
            details={"protective_stop_required": True},
            state=state,
            state_path=Path(args.state_path),
        )

    assert result is None
    assert state["protective_entry_stop"]["canceled_entry_order_count"] == 0
    signal = state["futures_recovery_runtime_safety_signal"]
    assert signal["generation"] == registered.generation
    assert signal["conditions"][0]["action_id"] == "runner_recover"
    mock_cancel_entries.assert_not_called()


@patch("grid_optimizer.loop_runner._cancel_futures_strategy_entry_orders")
def test_slow_cycle_protective_signal_blocks_all_local_gates_until_fresh_clear(
    mock_cancel_entries,
) -> None:
    with TemporaryDirectory() as tmpdir:
        args, _registered = _registered_args(tmpdir)
        args.sleep_seconds = 180.0
        state: dict[str, object] = {}
        _protective_entry_stop(
            args=args,
            symbol="BCHUSDT",
            strategy_mode=args.strategy_mode,
            api_key="unused",
            api_secret="unused",
            reason="reconcile_protective_stop",
            details={"protective_stop_required": True},
            state=state,
            state_path=Path(args.state_path),
        )
        signal = state[RUNTIME_SAFETY_SIGNAL_KEY]
        observed_at = datetime.fromisoformat(signal["observed_at"])
        hard_expires_at = datetime.fromisoformat(signal["hard_expires_at"])
        slow_next_cycle = observed_at + timedelta(seconds=181)

        assert (hard_expires_at - observed_at).total_seconds() >= 270.0

        plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {
                    "role": "best_quote_entry_long",
                    "side": "BUY",
                    "execution_type": "post_only",
                }
            ],
            "sell_orders": [],
        }
        plan_gate = _apply_registered_runtime_safety_signal_to_plan(
            args=args,
            plan=plan,
            now=slow_next_cycle,
        )
        assert plan_gate["active"] is True
        assert plan["buy_orders"] == []

        actions, action_gate = _apply_registered_runtime_safety_signal_to_actions(
            args=args,
            actions={
                "cancel_orders": [{"orderId": 1}],
                "place_orders": [
                    {
                        "role": "best_quote_entry_long",
                        "side": "BUY",
                        "execution_type": "post_only",
                    }
                ],
                "cancel_count": 1,
                "place_count": 1,
            },
            now=slow_next_cycle,
        )
        assert action_gate["active"] is True
        assert actions["cancel_count"] == 0
        assert actions["place_count"] == 0

        guarded, submit_reason = (
            _guard_registered_runtime_safety_order_before_submit(
                args=args,
                order={
                    "role": "best_quote_entry_long",
                    "side": "BUY",
                    "execution_type": "post_only",
                },
                now=slow_next_cycle,
            )
        )
        assert guarded is None
        assert submit_reason["reason"] == (
            "runtime_safety_signal_blocks_risk_increase"
        )

        local_state = json.loads(Path(args.state_path).read_text(encoding="utf-8"))
        assert _replace_registered_runtime_safety_source(
            args=args,
            state=local_state,
            state_path=Path(args.state_path),
            source="protective_entry_stop",
            conditions=(),
            now=slow_next_cycle + timedelta(seconds=1),
        )
        released_plan = {
            "bootstrap_orders": [],
            "buy_orders": [
                {
                    "role": "best_quote_entry_long",
                    "side": "BUY",
                    "execution_type": "post_only",
                }
            ],
            "sell_orders": [],
        }
        released_gate = _apply_registered_runtime_safety_signal_to_plan(
            args=args,
            plan=released_plan,
            now=slow_next_cycle + timedelta(seconds=2),
        )

    assert released_gate["active"] is False
    assert len(released_plan["buy_orders"]) == 1
    mock_cancel_entries.assert_not_called()


def test_coordinator_takeover_obsoletes_old_signal_and_only_coordinator_releases_state() -> None:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        args, registered = _registered_args(tmpdir)
        _normal_reports(output_dir, registered, NOW)
        state_path = Path(args.state_path)
        state_path.write_text(
            json.dumps(
                {
                    "futures_recovery_runtime_safety_signal": {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (
                            NOW + timedelta(seconds=120)
                        ).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": "safety_converge",
                                "reason": "rolling_hourly_loss_limit_hit",
                                "side": None,
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (
                                    NOW + timedelta(seconds=120)
                                ).isoformat(),
                                "details_digest": "sha256:" + "2" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        guard_state: dict[str, object] = {}
        restarts: list[str] = []

        entered = bq_volume_recovery_guard.run_registered_recovery_symbol_round(
            symbol="BCHUSDT",
            output_dir=output_dir,
            guard_state=guard_state,
            now=NOW + timedelta(seconds=1),
            window_seconds=60,
            min_volume_notional=10,
            near_cap_ratio=0.95,
            far_ticks=8,
            plan_stale_seconds=300,
            dry_run=False,
            runner_wrapper="/unused",
            runner_active_fetcher=lambda _symbol: True,
            restart_runner=restarts.append,
        )

        active = JsonRecoveryStore(Path(args.recovery_control_path)).read("BCHUSDT")
        args.recovery_generation = active.generation
        obsolete_actions, obsolete_report = (
            _apply_registered_runtime_safety_signal_to_actions(
                args=args,
                actions={
                    "cancel_orders": [],
                    "place_orders": [
                        {
                            "role": "best_quote_entry_long",
                            "side": "BUY",
                            "execution_type": "post_only",
                        }
                    ],
                    "cancel_count": 0,
                    "place_count": 1,
                },
                now=NOW + timedelta(seconds=2),
            )
        )
        assert obsolete_report["active"] is False
        assert obsolete_report["reason"] == "runtime_safety_signal_obsolete"
        assert obsolete_actions["place_count"] == 1
        local_state = json.loads(state_path.read_text(encoding="utf-8"))
        assert _replace_registered_runtime_safety_source(
            args=args,
            state=local_state,
            state_path=state_path,
            source="runtime_guard",
            conditions=(),
            now=NOW + timedelta(seconds=2),
        )
        # Local condition clearing is observation-only: it cannot toggle the
        # coordinator-owned action/profile back to normal.
        assert (
            JsonRecoveryStore(Path(args.recovery_control_path)).read("BCHUSDT")
            == active
        )
        assert RUNTIME_SAFETY_SIGNAL_KEY not in json.loads(
            state_path.read_text(encoding="utf-8")
        )
        _normal_reports(output_dir, active, NOW + timedelta(seconds=2))
        cleared = bq_volume_recovery_guard.run_registered_recovery_symbol_round(
            symbol="BCHUSDT",
            output_dir=output_dir,
            guard_state=guard_state,
            now=NOW + timedelta(seconds=3),
            window_seconds=60,
            min_volume_notional=10,
            near_cap_ratio=0.95,
            far_ticks=8,
            plan_stale_seconds=300,
            dry_run=False,
            runner_wrapper="/unused",
            runner_active_fetcher=lambda _symbol: True,
            restart_runner=restarts.append,
        )

    assert entered["action"] == "coordinator_safety_converge_enter"
    assert entered["effect_count"] == 1
    assert entered["control_cas_count"] == 1
    assert entered["snapshot_count"] == 1
    assert cleared["action"] == "coordinator_safety_converge_exit"
    assert cleared["effect_count"] == 1
    assert cleared["control_cas_count"] == 1
    assert restarts == ["BCHUSDT", "BCHUSDT"]
    assert ActionId(cleared["active_action"]) is ActionId.SAFETY_CONVERGE


def _run_bq_signal_action(
    *,
    action_id: str,
    reason: str,
    side: str | None,
) -> dict[str, object]:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        args, registered = _registered_args(tmpdir)
        _normal_reports(output_dir, registered, NOW)
        Path(args.state_path).write_text(
            json.dumps(
                {
                    "futures_recovery_runtime_safety_signal": {
                        "schema": "futures_recovery_runtime_safety_signal_v1",
                        "symbol": "BCHUSDT",
                        "run_contract_id": run_contract_identity_from_config(vars(args)),
                        "generation": registered.generation,
                        "decision_id": registered.decision_id,
                        "observed_at": NOW.isoformat(),
                        "hard_expires_at": (
                            NOW + timedelta(seconds=120)
                        ).isoformat(),
                        "conditions": [
                            {
                                "source": "runtime_guard",
                                "action_id": action_id,
                                "reason": reason,
                                "side": side,
                                "observed_at": NOW.isoformat(),
                                "hard_expires_at": (
                                    NOW + timedelta(seconds=120)
                                ).isoformat(),
                                "details_digest": "sha256:" + "4" * 64,
                            }
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        restarts: list[str] = []
        result = bq_volume_recovery_guard.run_registered_recovery_symbol_round(
            symbol="BCHUSDT",
            output_dir=output_dir,
            guard_state={},
            now=NOW + timedelta(seconds=1),
            window_seconds=60,
            min_volume_notional=10,
            near_cap_ratio=0.95,
            far_ticks=8,
            plan_stale_seconds=300,
            dry_run=False,
            runner_wrapper="/unused",
            runner_active_fetcher=lambda _symbol: True,
            restart_runner=restarts.append,
        )
        result["restarts"] = restarts
        return result


def test_bq_maps_runtime_position_cap_signal_to_inventory_recovery() -> None:
    result = _run_bq_signal_action(
        action_id="inventory_recover",
        reason="max_actual_net_notional_hit",
        side="SELL",
    )

    assert result["action"] == "coordinator_inventory_recover_enter"
    assert result["side"] == "SELL"
    assert result["effect_count"] == 1
    assert result["control_cas_count"] == 1
    assert result["restarts"] == ["BCHUSDT"]


def test_bq_maps_protective_signal_to_runner_recovery() -> None:
    result = _run_bq_signal_action(
        action_id="runner_recover",
        reason="reconcile_protective_stop",
        side=None,
    )

    assert result["action"] == "coordinator_runner_recover_enter"
    assert result["effect_count"] == 1
    assert result["control_cas_count"] == 1
    assert result["restarts"] == ["BCHUSDT"]


@patch("grid_optimizer.loop_runner._cancel_futures_strategy_entry_orders")
def test_slow_signal_survives_coordinator_poll_and_one_generation_handoff(
    mock_cancel_entries,
) -> None:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        args, registered = _registered_args(tmpdir)
        args.sleep_seconds = 180.0
        local_state: dict[str, object] = {}
        _protective_entry_stop(
            args=args,
            symbol="BCHUSDT",
            strategy_mode=args.strategy_mode,
            api_key="unused",
            api_secret="unused",
            reason="reconcile_protective_stop",
            details={"protective_stop_required": True},
            state=local_state,
            state_path=Path(args.state_path),
        )
        observed_at = datetime.fromisoformat(
            local_state[RUNTIME_SAFETY_SIGNAL_KEY]["observed_at"]
        )
        _normal_reports(output_dir, registered, observed_at)
        guard_state: dict[str, object] = {}
        restarts: list[str] = []

        entered = bq_volume_recovery_guard.run_registered_recovery_symbol_round(
            symbol="BCHUSDT",
            output_dir=output_dir,
            guard_state=guard_state,
            now=observed_at + timedelta(seconds=61),
            window_seconds=60,
            min_volume_notional=10,
            near_cap_ratio=0.95,
            far_ticks=8,
            plan_stale_seconds=300,
            dry_run=False,
            runner_wrapper="/unused",
            runner_active_fetcher=lambda _symbol: True,
            restart_runner=restarts.append,
        )
        active = JsonRecoveryStore(Path(args.recovery_control_path)).read("BCHUSDT")
        _normal_reports(output_dir, active, observed_at + timedelta(seconds=120))
        held = bq_volume_recovery_guard.run_registered_recovery_symbol_round(
            symbol="BCHUSDT",
            output_dir=output_dir,
            guard_state=guard_state,
            now=observed_at + timedelta(seconds=121),
            window_seconds=60,
            min_volume_notional=10,
            near_cap_ratio=0.95,
            far_ticks=8,
            plan_stale_seconds=300,
            dry_run=False,
            runner_wrapper="/unused",
            runner_active_fetcher=lambda _symbol: True,
            restart_runner=restarts.append,
        )
        active = JsonRecoveryStore(Path(args.recovery_control_path)).read("BCHUSDT")
        _normal_reports(output_dir, active, observed_at + timedelta(seconds=270))
        expired = bq_volume_recovery_guard.run_registered_recovery_symbol_round(
            symbol="BCHUSDT",
            output_dir=output_dir,
            guard_state=guard_state,
            now=observed_at + timedelta(seconds=271),
            window_seconds=60,
            min_volume_notional=10,
            near_cap_ratio=0.95,
            far_ticks=8,
            plan_stale_seconds=300,
            dry_run=False,
            runner_wrapper="/unused",
            runner_active_fetcher=lambda _symbol: True,
            restart_runner=restarts.append,
        )

    assert entered["action"] == "coordinator_runner_recover_enter"
    assert entered["runtime_safety_signal_status"] == "fresh"
    assert held["action"] == "coordinator_runner_recover_hold"
    assert held["runtime_safety_signal_status"] == "fresh_predecessor"
    assert held["effect_count"] == 0
    assert expired["action"] == "coordinator_runner_recover_exit"
    assert expired["runtime_safety_signal_status"] == "stale"
    assert restarts == ["BCHUSDT"]
    mock_cancel_entries.assert_not_called()
