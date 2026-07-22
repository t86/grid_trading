from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from grid_optimizer.futures_recovery_coordinator import RecoveryState
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_KEY,
    RECOVERY_STATE_MIRROR_KEY,
    RECOVERY_STATE_SCHEMA_VERSION,
    JsonRecoveryStore,
)
from grid_optimizer.recovery_coordinator_watchdog import (
    check_symbol,
    main,
    assess_coordinator_liveness,
    update_watchdog_state,
)


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


def test_watchdog_has_no_runner_or_order_actuator() -> None:
    source = Path("src/grid_optimizer/recovery_coordinator_watchdog.py").read_text(
        encoding="utf-8"
    )

    assert "systemctl" not in source
    assert "subprocess" not in source
    assert "runner_wrapper" not in source
    assert "place_order" not in source
