from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from grid_optimizer.competition_target_gate import submit_lifecycle_intent
from grid_optimizer.futures_recovery_coordinator import RecoveryState
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_KEY,
    RECOVERY_STATE_MIRROR_KEY,
    RECOVERY_STATE_SCHEMA_VERSION,
    JsonRecoveryStore,
)
from grid_optimizer.futures_run_lifecycle import (
    bind_run_contract_owner,
    resolve_authoritative_run_contract,
)
from grid_optimizer.recovery_coordinator_watchdog import (
    check_symbol,
    main,
    assess_coordinator_liveness,
    update_watchdog_state,
)
import grid_optimizer.recovery_coordinator_watchdog as watchdog


NOW = datetime(2026, 7, 22, 8, 0, tzinfo=timezone.utc)


def _registered_control(symbol: str = "BCHUSDT") -> dict[str, object]:
    state = RecoveryState.initial(
        symbol,
        {
            "best_quote_maker_volume_allow_loss_reduce_only": False,
            "best_quote_maker_volume_net_loss_reduce_enabled": False,
            "hard_loss_forced_reduce_enabled": False,
            "volatility_entry_pause_enabled": True,
        },
        now=NOW,
    )
    encoded = JsonRecoveryStore.encode_state(state)
    envelope = {"schema_version": RECOVERY_STATE_SCHEMA_VERSION, "state": encoded}
    control = dict(encoded["desired_profile"]["fields"])
    control[RECOVERY_STATE_KEY] = envelope
    control[RECOVERY_STATE_MIRROR_KEY] = envelope
    return control


def _healthy_heartbeat(*, checked_at: datetime = NOW) -> dict[str, object]:
    return {
        "futures_recovery_guard_heartbeat": {
            "schema": "futures_recovery_guard_heartbeat_v1",
            "checked_at": checked_at.isoformat(),
            "ok": True,
            "symbols": {"BCHUSDT": {"healthy": True, "action": "coordinator_noop_hold"}},
        }
    }


def _active_target_control(*, target: float = 20_000.0) -> dict[str, object]:
    control = _registered_control()
    control.update(
        {
            "symbol": "BCHUSDT",
            "strategy_profile": "test_profile",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "per_order_notional": 20.0,
            "run_start_time": "2026-07-22T07:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-07-22T07:00:00+00:00",
            "run_end_time": "2026-07-22T09:00:00+00:00",
            "max_cumulative_notional": target,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 2.0,
            "terminal_drain_max_wait_seconds": 600.0,
        }
    )
    owned, _ = bind_run_contract_owner(control, activated_at=NOW)
    return owned


def test_registered_symbol_requires_a_fresh_successful_coordinator_heartbeat() -> None:
    healthy = assess_coordinator_liveness(
        symbol="BCHUSDT",
        control=_registered_control(),
        guard_state=_healthy_heartbeat(),
        now=NOW,
        max_heartbeat_age_seconds=150,
    )
    stale = assess_coordinator_liveness(
        symbol="BCHUSDT",
        control=_registered_control(),
        guard_state=_healthy_heartbeat(checked_at=NOW - timedelta(seconds=151)),
        now=NOW,
        max_heartbeat_age_seconds=150,
    )

    assert healthy["tracked"] is True
    assert healthy["healthy"] is True
    assert stale["healthy"] is False
    assert stale["reason"] == "coordinator_heartbeat_stale"


def test_blocked_symbol_alerts_without_marking_other_symbol_unhealthy() -> None:
    blocked_heartbeat = {
        "futures_recovery_guard_heartbeat": {
            "schema": "futures_recovery_guard_heartbeat_v1",
            "checked_at": NOW.isoformat(),
            "ok": False,
            "symbols": {
                "BCHUSDT": {
                    "healthy": True,
                    "action": "coordinator_noop_hold",
                    "liveness_status": "healthy",
                },
                "ARXUSDT": {
                    "healthy": False,
                    "action": "coordinator_safety_converge_hold",
                    "liveness_status": "blocked",
                },
            },
        }
    }

    healthy = assess_coordinator_liveness(
        symbol="BCHUSDT",
        control=_registered_control("BCHUSDT"),
        guard_state=blocked_heartbeat,
        now=NOW,
        max_heartbeat_age_seconds=150,
    )
    blocked = assess_coordinator_liveness(
        symbol="ARXUSDT",
        control=_registered_control("ARXUSDT"),
        guard_state=blocked_heartbeat,
        now=NOW,
        max_heartbeat_age_seconds=150,
    )

    assert healthy["healthy"] is True
    assert healthy["reason"] == "coordinator_heartbeat_fresh"
    assert blocked["healthy"] is False
    assert blocked["reason"] == "coordinator_symbol_blocked"
    assert blocked["action"] == "coordinator_safety_converge_hold"


def test_unregistered_symbol_is_explicitly_observed_without_a_false_alarm() -> None:
    assessment = assess_coordinator_liveness(
        symbol="BCHUSDT",
        control={},
        guard_state={},
        now=NOW,
        max_heartbeat_age_seconds=150,
    )

    assert assessment == {
        "symbol": "BCHUSDT",
        "tracked": False,
        "healthy": True,
        "reason": "unregistered",
    }


def test_failure_alert_is_deduplicated_and_a_new_forced_service_failure_alerts_now() -> None:
    assessment = assess_coordinator_liveness(
        symbol="BCHUSDT",
        control=_registered_control(),
        guard_state={},
        now=NOW,
        max_heartbeat_age_seconds=150,
    )
    state, first_alert = update_watchdog_state(
        assessment=assessment, state={}, now=NOW, alert_threshold=2
    )
    state, second_alert = update_watchdog_state(
        assessment=assessment, state=state, now=NOW + timedelta(minutes=1), alert_threshold=2
    )
    forced = assess_coordinator_liveness(
        symbol="BCHUSDT",
        control=_registered_control(),
        guard_state=_healthy_heartbeat(),
        now=NOW + timedelta(minutes=2),
        max_heartbeat_age_seconds=150,
        force_reason="coordinator_service_failed",
    )
    state, forced_alert = update_watchdog_state(
        assessment=forced,
        state=state,
        now=NOW + timedelta(minutes=2),
        alert_threshold=2,
        force_alert=True,
    )

    assert first_alert is False
    assert second_alert is True
    assert forced_alert is True
    assert state["BCHUSDT"]["reason"] == "coordinator_service_failed"


def test_main_writes_a_fresh_watchdog_completion_heartbeat(tmp_path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    alert_config_path = output_dir / "alert.json"
    alert_config_path.write_text(
        json.dumps({"email_to": ["ops@example.com"]}), encoding="utf-8"
    )
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control()), encoding="utf-8"
    )
    guard_state_path = output_dir / "guard.json"
    guard_state_path.write_text(
        json.dumps(_healthy_heartbeat(checked_at=datetime.now(timezone.utc))),
        encoding="utf-8",
    )
    state_path = output_dir / "watchdog.json"

    assert main([
        "--symbols", "BCHUSDT", "--output-dir", str(output_dir),
        "--guard-state-path", str(guard_state_path), "--state-path", str(state_path),
        "--alert-config-path", str(alert_config_path),
    ]) == 0

    heartbeat = json.loads(state_path.read_text(encoding="utf-8"))["recovery_coordinator_watchdog_heartbeat"]
    assert heartbeat["schema"] == "recovery_coordinator_watchdog_heartbeat_v1"
    assert heartbeat["ok"] is True
    assert heartbeat["symbols"] == {"BCHUSDT": True}


def test_registered_symbol_with_no_alert_delivery_is_unhealthy(tmp_path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control()), encoding="utf-8"
    )

    result = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=_healthy_heartbeat(),
        state={},
        now=NOW,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=output_dir / "missing-alert.json",
    )

    assert result["assessment"]["healthy"] is False
    assert result["assessment"]["reason"] == "alert_delivery_not_configured"


def test_active_target_window_requires_a_fresh_target_gate_heartbeat(tmp_path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _active_target_control()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    monkeypatch.setattr(
        watchdog, "load_alert_notifier_config", lambda _path: {"enabled": True}
    )

    result = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=_healthy_heartbeat(),
        state={},
        now=NOW,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=output_dir / "alert.json",
    )

    assert result["assessment"]["healthy"] is False
    assert result["assessment"]["reason"] == "target_gate_heartbeat_missing"

    _snapshot, run_contract_id = resolve_authoritative_run_contract(
        control, expected_symbol="BCHUSDT"
    )
    (output_dir / "bchusdt_target_gate_heartbeat.json").write_text(
        json.dumps(
            {
                "schema": "futures_target_gate_heartbeat_v1",
                "symbol": "BCHUSDT",
                "run_contract_id": run_contract_id,
                "checked_at": NOW.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    recovered = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=_healthy_heartbeat(),
        state={},
        now=NOW,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=output_dir / "alert.json",
    )

    assert recovered["assessment"]["healthy"] is True
    assert recovered["target_gate"]["reason"] == "target_gate_heartbeat_fresh"


def test_deadline_requires_current_contract_terminal_intent(tmp_path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _active_target_control()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    monkeypatch.setattr(
        watchdog, "load_alert_notifier_config", lambda _path: {"enabled": True}
    )
    deadline_now = NOW + timedelta(hours=2)

    missing = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=_healthy_heartbeat(checked_at=deadline_now),
        state={},
        now=deadline_now,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=output_dir / "alert.json",
    )

    assert missing["assessment"]["healthy"] is False
    assert missing["assessment"]["reason"] == "deadline_terminal_intent_missing"

    snapshot, _run_contract_id = resolve_authoritative_run_contract(
        control, expected_symbol="BCHUSDT"
    )
    intent, created = submit_lifecycle_intent(
        workdir=str(tmp_path),
        symbol="BCHUSDT",
        trigger_reason="target_unmet_deadline",
        requested_at=deadline_now.isoformat(),
        observed={
            "gross_notional": 100.0,
            "target": float(snapshot["max_cumulative_notional"]),
            "realized_pnl": 0.0,
            "wear_per_10k": 0.0,
            "trade_count": 1,
            "window_start": snapshot["runtime_guard_stats_start_time"],
            "window_end": snapshot["run_end_time"],
            "query_end": snapshot["run_end_time"],
            "runtime_guard_primary_reason": "after_end_window",
            "runtime_guard_matched_reasons": ["after_end_window"],
        },
        run_contract_config=control,
    )
    assert created is True
    assert intent["run_contract_id"] == _run_contract_id

    recovered = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=_healthy_heartbeat(checked_at=deadline_now),
        state={},
        now=deadline_now,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=output_dir / "alert.json",
    )

    assert recovered["assessment"]["healthy"] is True
    assert recovered["deadline_terminal"]["reason"] == "deadline_terminal_intent_present"


def test_deadline_rejects_other_contract_terminal_intent(tmp_path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    current_control = _active_target_control()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(current_control), encoding="utf-8"
    )
    monkeypatch.setattr(
        watchdog, "load_alert_notifier_config", lambda _path: {"enabled": True}
    )
    deadline_now = NOW + timedelta(hours=2)
    old_control = _active_target_control(target=19_000.0)
    old_snapshot, old_contract_id = resolve_authoritative_run_contract(
        old_control, expected_symbol="BCHUSDT"
    )
    intent, created = submit_lifecycle_intent(
        workdir=str(tmp_path),
        symbol="BCHUSDT",
        trigger_reason="target_unmet_deadline",
        requested_at=deadline_now.isoformat(),
        observed={
            "gross_notional": 100.0,
            "target": float(old_snapshot["max_cumulative_notional"]),
            "realized_pnl": 0.0,
            "wear_per_10k": 0.0,
            "trade_count": 1,
            "window_start": old_snapshot["runtime_guard_stats_start_time"],
            "window_end": old_snapshot["run_end_time"],
            "query_end": old_snapshot["run_end_time"],
            "runtime_guard_primary_reason": "after_end_window",
            "runtime_guard_matched_reasons": ["after_end_window"],
        },
        run_contract_config=old_control,
    )
    assert created is True
    assert intent["run_contract_id"] == old_contract_id

    result = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state=_healthy_heartbeat(checked_at=deadline_now),
        state={},
        now=deadline_now,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=output_dir / "alert.json",
    )

    assert result["assessment"]["healthy"] is False
    assert result["assessment"]["reason"] == "deadline_terminal_intent_contract_mismatch"


def test_watchdog_alerts_when_a_present_control_file_is_unreadable(tmp_path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        "{invalid-json", encoding="utf-8"
    )
    monkeypatch.setattr(
        watchdog, "load_alert_notifier_config", lambda _path: {"enabled": True}
    )
    monkeypatch.setattr(
        watchdog, "send_alert_email", lambda **_kwargs: {"sent": True}
    )

    result = check_symbol(
        symbol="BCHUSDT",
        output_dir=output_dir,
        guard_state={},
        state={},
        now=NOW,
        max_heartbeat_age_seconds=150,
        alert_threshold=1,
        alert_config_path=None,
    )

    assert result["assessment"]["tracked"] is True
    assert result["assessment"]["healthy"] is False
    assert result["assessment"]["reason"] == "recovery_control_unreadable"
    assert result["alert"] == {"sent": True}


def test_watchdog_has_no_runner_or_order_actuator() -> None:
    source = Path("src/grid_optimizer/recovery_coordinator_watchdog.py").read_text(
        encoding="utf-8"
    )

    assert "systemctl" not in source
    assert "subprocess" not in source
    assert "runner_wrapper" not in source
    assert "place_order" not in source
