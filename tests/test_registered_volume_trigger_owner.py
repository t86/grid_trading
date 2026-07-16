from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer import bq_volume_recovery_guard, web
from grid_optimizer.futures_recovery_coordinator import ActionId, RecoveryPhase
from grid_optimizer.futures_recovery_runtime_adapter import (
    RecoveryConfigAppliedEvidence,
)
from grid_optimizer.futures_run_lifecycle import bind_run_contract_owner
from grid_optimizer.futures_recovery_store import JsonRecoveryStore
from grid_optimizer.futures_volatility_safety_observation import (
    VOLATILITY_SAFETY_OBSERVATION_MAX_AGE,
    decode_volatility_safety_observation,
)
from grid_optimizer.futures_volume_safety_observation import (
    VOLUME_SAFETY_OBSERVATION_MAX_AGE,
    decode_volume_safety_observation,
)


NOW = datetime(2026, 7, 16, 2, tzinfo=timezone.utc)


def _registered_bch_config(output_dir: Path) -> tuple[dict[str, object], object]:
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    config: dict[str, object] = {
        "symbol": "BCHUSDT",
        "strategy_profile": "bch-volume-owner-test-v1",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "state_path": str(output_dir / "bchusdt_loop_state.json"),
        "plan_json": str(output_dir / "bchusdt_loop_latest_plan.json"),
        "submit_report_json": str(output_dir / "bchusdt_loop_latest_submit.json"),
        "volume_trigger_enabled": True,
        "volume_trigger_window": "5m",
        "volume_trigger_start_threshold": 200.0,
        "volume_trigger_stop_threshold": 100.0,
        "best_quote_maker_volume_allow_loss_reduce_only": False,
        "best_quote_maker_volume_net_loss_reduce_enabled": False,
        "hard_loss_forced_reduce_enabled": False,
        "volatility_entry_pause_enabled": True,
    }
    control_path.write_text(json.dumps(config), encoding="utf-8")
    state = JsonRecoveryStore(control_path).register_symbol(
        "BCHUSDT",
        config,
        now=NOW - timedelta(minutes=1),
    )
    return json.loads(control_path.read_text(encoding="utf-8")), state


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


def _run_guard(
    output_dir: Path,
    *,
    state: object,
    now: datetime,
    runner_active: bool,
    restarts: list[str],
    guard_state: dict[str, object],
) -> dict[str, object]:
    _normal_reports(output_dir, state, now)
    return bq_volume_recovery_guard.run_registered_recovery_symbol_round(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=guard_state,
        now=now,
        window_seconds=60,
        min_volume_notional=10,
        near_cap_ratio=0.95,
        far_ticks=8,
        plan_stale_seconds=300,
        dry_run=False,
        runner_wrapper="/unused",
        runner_active_fetcher=lambda _symbol: runner_active,
        restart_runner=restarts.append,
    )


def _observe_volume(
    config: dict[str, object],
    status_path: Path,
    *,
    quote_volume: float,
    runner_running: bool,
) -> dict[str, object]:
    with (
        patch(
            "grid_optimizer.web.fetch_futures_quote_volume_sum",
            return_value=quote_volume,
        ),
        patch(
            "grid_optimizer.web._read_runner_process_for_symbol",
            return_value={"is_running": runner_running},
        ),
        patch(
            "grid_optimizer.web._read_flatten_process_for_symbol",
            return_value={"is_running": False},
        ),
        patch(
            "grid_optimizer.web._volume_trigger_status_path",
            return_value=status_path,
        ),
        patch("grid_optimizer.web._start_runner_process") as start_runner,
        patch("grid_optimizer.web._stop_runner_process") as stop_runner,
    ):
        web._reconcile_runner_volume_trigger(config)
    start_runner.assert_not_called()
    stop_runner.assert_not_called()
    return json.loads(status_path.read_text(encoding="utf-8"))


def _registered_volatility_config(
    output_dir: Path,
) -> tuple[dict[str, object], object]:
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    config: dict[str, object] = {
        "symbol": "BCHUSDT",
        "strategy_profile": "bch-volatility-owner-test-v1",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T01:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T01:00:00+00:00",
        "run_end_time": "2026-07-16T04:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
        "volatility_trigger_enabled": True,
        "volatility_trigger_window": "15m",
        "volatility_trigger_amplitude_ratio": 0.02,
        "volatility_trigger_abs_return_ratio": 0.01,
        "best_quote_maker_volume_allow_loss_reduce_only": False,
        "best_quote_maker_volume_net_loss_reduce_enabled": False,
        "hard_loss_forced_reduce_enabled": False,
        "volatility_entry_pause_enabled": True,
    }
    config, _changed = bind_run_contract_owner(
        config,
        activated_at=NOW - timedelta(minutes=2),
    )
    control_path.write_text(json.dumps(config), encoding="utf-8")
    state = JsonRecoveryStore(control_path).register_symbol(
        "BCHUSDT",
        config,
        now=NOW - timedelta(minutes=1),
    )
    return json.loads(control_path.read_text(encoding="utf-8")), state


def _observe_volatility(
    config: dict[str, object],
    status_path: Path,
) -> dict[str, object]:
    with (
        patch(
            "grid_optimizer.web.fetch_futures_window_price_stats",
            return_value={"amplitude_ratio": 0.05, "return_ratio": -0.03},
        ),
        patch(
            "grid_optimizer.web._read_runner_process_for_symbol",
            return_value={"is_running": True},
        ),
        patch(
            "grid_optimizer.web._read_flatten_process_for_symbol",
            return_value={"is_running": False},
        ),
        patch(
            "grid_optimizer.web._runner_volatility_trigger_status",
            return_value={},
        ),
        patch(
            "grid_optimizer.web._volatility_trigger_status_path",
            return_value=status_path,
        ),
        patch("grid_optimizer.web._start_runner_process") as start_runner,
        patch("grid_optimizer.web._stop_runner_process") as stop_runner,
    ):
        web._reconcile_runner_volatility_trigger(config)
    start_runner.assert_not_called()
    stop_runner.assert_not_called()
    return json.loads(status_path.read_text(encoding="utf-8"))


def test_registered_volatility_observation_expires_and_restores_without_renewal() -> None:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config, initial = _registered_volatility_config(output_dir)
        status_path = output_dir / "bchusdt_volatility_trigger_status.json"
        status = _observe_volatility(config, status_path)
        observation = decode_volatility_safety_observation(
            status["orchestrator_observation"]
        )
        assert observation.active is True
        assert (
            observation.hard_expires_at - observation.observed_at
            == VOLATILITY_SAFETY_OBSERVATION_MAX_AGE
        )
        restarts: list[str] = []
        guard_state: dict[str, object] = {}

        entered_result = _run_guard(
            output_dir,
            state=initial,
            now=observation.observed_at + timedelta(seconds=1),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )
        entered = JsonRecoveryStore(
            output_dir / "bchusdt_loop_runner_control.json"
        ).read("BCHUSDT")
        original_expiry = entered.safety_lease.hard_expires_at
        assert entered_result["safety_observation_status"] == "fresh_active"
        assert entered.active_action is ActionId.SAFETY_CONVERGE

        stale_at = observation.hard_expires_at + timedelta(seconds=1)
        stale_result = _run_guard(
            output_dir,
            state=entered,
            now=stale_at,
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )
        restoring = JsonRecoveryStore(
            output_dir / "bchusdt_loop_runner_control.json"
        ).read("BCHUSDT")
        assert stale_result["safety_observation_status"] == "stale"
        assert restoring.phase is RecoveryPhase.RESTORING
        assert restoring.safety_lease is not None
        assert restoring.safety_lease.hard_expires_at == original_expiry

        applied = RecoveryConfigAppliedEvidence(
            applied_generation=restoring.generation,
            applied_profile_digest=restoring.desired_profile.digest,
            receipt_id="volatility-restore",
            runner_instance_id="runner-volatility-restore",
            observed_at=stale_at + timedelta(seconds=1),
            latest_observation_seq=1,
        )
        with patch.object(
            bq_volume_recovery_guard,
            "load_recovery_config_applied_evidence",
            return_value=applied,
        ):
            restored_result = _run_guard(
                output_dir,
                state=restoring,
                now=stale_at + timedelta(seconds=1),
                runner_active=True,
                restarts=restarts,
                guard_state=guard_state,
            )
        restored = JsonRecoveryStore(
            output_dir / "bchusdt_loop_runner_control.json"
        ).read("BCHUSDT")
        assert restored_result["safety_observation_status"] == "obsolete"
        assert restored.phase is RecoveryPhase.COOLDOWN
        assert restored.desired_profile.digest == restored.baseline_profile.digest


def test_registered_non_arx_low_volume_is_persisted_and_safety_wins_runner_fault() -> None:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config, initial = _registered_bch_config(output_dir)
        status_path = output_dir / "bchusdt_volume_trigger_status.json"

        status = _observe_volume(
            config,
            status_path,
            quote_volume=20.0,
            runner_running=False,
        )
        observation = decode_volume_safety_observation(
            status["orchestrator_observation"]
        )
        restarts: list[str] = []
        guard_state: dict[str, object] = {}
        entered = _run_guard(
            output_dir,
            state=initial,
            now=observation.observed_at + timedelta(seconds=1),
            runner_active=False,
            restarts=restarts,
            guard_state=guard_state,
        )

    assert status["requested_action"] == "entry_pause"
    assert status["action"] is None
    assert observation.symbol == "BCHUSDT"
    assert observation.active is True
    assert observation.reason == "volume_below_stop_threshold"
    assert observation.hard_expires_at == (
        observation.observed_at + VOLUME_SAFETY_OBSERVATION_MAX_AGE
    )
    assert entered["action"] == "coordinator_safety_converge_enter"
    assert entered["active_action"] == ActionId.SAFETY_CONVERGE.value
    assert entered["volume_safety_observation_status"] == "fresh_active"
    assert entered["runner_active"] is False


def test_registered_volume_recovery_is_observation_only_until_coordinator_exits() -> None:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config, initial = _registered_bch_config(output_dir)
        control_path = output_dir / "bchusdt_loop_runner_control.json"
        status_path = output_dir / "bchusdt_volume_trigger_status.json"
        active_status = _observe_volume(
            config,
            status_path,
            quote_volume=20.0,
            runner_running=True,
        )
        active_observation = decode_volume_safety_observation(
            active_status["orchestrator_observation"]
        )
        restarts: list[str] = []
        guard_state: dict[str, object] = {}
        _run_guard(
            output_dir,
            state=initial,
            now=active_observation.observed_at + timedelta(seconds=1),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )
        active_state = JsonRecoveryStore(control_path).read("BCHUSDT")
        assert active_state.active_action is ActionId.SAFETY_CONVERGE

        refreshed_config = json.loads(control_path.read_text(encoding="utf-8"))
        clear_status = _observe_volume(
            refreshed_config,
            status_path,
            quote_volume=150.0,
            runner_running=True,
        )
        clear_observation = decode_volume_safety_observation(
            clear_status["orchestrator_observation"]
        )
        # Web only replaces its observation.  It cannot clear coordinator state.
        assert JsonRecoveryStore(control_path).read("BCHUSDT") == active_state
        exited = _run_guard(
            output_dir,
            state=active_state,
            now=clear_observation.observed_at + timedelta(seconds=3),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )

    assert clear_observation.active is False
    assert clear_status["requested_action"] is None
    assert exited["action"] == "coordinator_safety_converge_exit"
    assert exited["volume_safety_observation_status"] == "fresh_clear"


def test_registered_volume_safety_absolute_ttl_releases_without_refresh() -> None:
    with TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config, initial = _registered_bch_config(output_dir)
        control_path = output_dir / "bchusdt_loop_runner_control.json"
        status_path = output_dir / "bchusdt_volume_trigger_status.json"
        status = _observe_volume(
            config,
            status_path,
            quote_volume=20.0,
            runner_running=True,
        )
        observation = decode_volume_safety_observation(
            status["orchestrator_observation"]
        )
        restarts: list[str] = []
        guard_state: dict[str, object] = {}
        _run_guard(
            output_dir,
            state=initial,
            now=observation.observed_at + timedelta(seconds=1),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )
        active_state = JsonRecoveryStore(control_path).read("BCHUSDT")
        still_active_61 = _run_guard(
            output_dir,
            state=active_state,
            now=observation.observed_at + timedelta(seconds=61),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )
        active_state = JsonRecoveryStore(control_path).read("BCHUSDT")
        still_active_90 = _run_guard(
            output_dir,
            state=active_state,
            now=observation.observed_at + timedelta(seconds=90),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )
        active_state = JsonRecoveryStore(control_path).read("BCHUSDT")
        exited = _run_guard(
            output_dir,
            state=active_state,
            now=observation.hard_expires_at + timedelta(microseconds=1),
            runner_active=True,
            restarts=restarts,
            guard_state=guard_state,
        )

    assert still_active_61["active_action"] == ActionId.SAFETY_CONVERGE.value
    assert still_active_61["volume_safety_observation_status"] == "fresh_active"
    assert still_active_90["active_action"] == ActionId.SAFETY_CONVERGE.value
    assert still_active_90["volume_safety_observation_status"] == "fresh_active"
    assert exited["action"] == "coordinator_safety_converge_exit"
    assert exited["volume_safety_observation_status"] == "stale"


def test_volume_poll_error_preserves_prior_absolute_expiry() -> None:
    prior_observation = {
        "schema": "futures_volume_safety_observation_v1",
        "hard_expires_at": "2026-07-16T02:02:00+00:00",
    }

    class _StopAfterOneWait:
        stopped = False

        def is_set(self) -> bool:
            return self.stopped

        def wait(self, _seconds: float) -> bool:
            self.stopped = True
            return True

    with (
        patch(
            "grid_optimizer.web._iter_saved_runner_control_configs",
            return_value=[
                {
                    "symbol": "BCHUSDT",
                    "volume_trigger_enabled": True,
                    "volume_trigger_window": "5m",
                    "volume_trigger_stop_threshold": 100.0,
                    "_futures_recovery_state": {"schema_version": 1},
                }
            ],
        ),
        patch(
            "grid_optimizer.web.fetch_futures_quote_volume_sum",
            side_effect=RuntimeError("market data unavailable"),
        ),
        patch(
            "grid_optimizer.web._runner_volume_trigger_status",
            return_value={
                "orchestrator_observation": prior_observation,
                "requested_action": "entry_pause",
            },
        ),
        patch("grid_optimizer.web._update_volume_trigger_status") as update_status,
    ):
        web._run_volume_trigger_loop(_StopAfterOneWait())

    error_status = update_status.call_args.args[1]
    assert error_status["reason"] == "error"
    assert error_status["orchestrator_observation"] == prior_observation
    assert (
        error_status["orchestrator_observation"]["hard_expires_at"]
        == "2026-07-16T02:02:00+00:00"
    )
