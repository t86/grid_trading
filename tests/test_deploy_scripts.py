from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from grid_optimizer.futures_recovery_coordinator import RecoveryState
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_KEY,
    RECOVERY_STATE_MIRROR_KEY,
    RECOVERY_STATE_SCHEMA_VERSION,
    JsonRecoveryStore,
)
from grid_optimizer.futures_run_lifecycle import (
    bind_run_contract_owner,
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
)
from grid_optimizer.futures_terminal_drain import (
    DrainStage,
    TerminalDrainState,
    terminal_drain_state_to_dict,
)
from grid_optimizer.futures_terminal_ownership import terminal_intent_id
from grid_optimizer.loop_runner import (
    _terminal_drain_exit_contract_from_snapshot,
    _terminal_drain_owner_integrity_digest,
    _terminal_drain_runtime_integrity_digest,
)


def _owned_control(control: dict[str, object]) -> dict[str, object]:
    owned, _ = bind_run_contract_owner(
        control,
        activated_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    return owned


def _registered_control(control: dict[str, object]) -> dict[str, object]:
    state = RecoveryState.initial(
        str(control["symbol"]),
        control,
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    encoded_state = JsonRecoveryStore.encode_state(state)
    envelope = {
        "schema_version": RECOVERY_STATE_SCHEMA_VERSION,
        "state": encoded_state,
    }
    registered = dict(encoded_state["desired_profile"]["fields"])
    registered[RECOVERY_STATE_KEY] = envelope
    registered[RECOVERY_STATE_MIRROR_KEY] = envelope
    return registered


def _terminal_runtime_owner(
    control: dict[str, object],
    *,
    exit_status: str,
) -> dict[str, object]:
    snapshot = run_contract_snapshot_from_config(control)
    contract_id = run_contract_identity_from_config(snapshot)
    symbol = str(snapshot["symbol"])
    decision_id = f"{symbol}|{contract_id}"
    drain_state = TerminalDrainState.initial(symbol, decision_id=decision_id)
    if exit_status == "stopped_clean":
        drain_state = replace(drain_state, stage=DrainStage.STOPPED_CLEAN)
    elif exit_status == "stopped_preserved":
        drain_state = replace(drain_state, stage=DrainStage.STOPPED_PRESERVED)
    started_at = datetime(2026, 7, 16, tzinfo=timezone.utc).isoformat()
    owner: dict[str, object] = {
        "schema": "futures_terminal_drain_runtime_v1",
        "decision_id": decision_id,
        "run_contract_id": contract_id,
        "run_contract_snapshot": snapshot,
        "started_at": started_at,
        "exit_status": exit_status,
        "exit_contract": _terminal_drain_exit_contract_from_snapshot(
            snapshot,
            captured_at=started_at,
        ),
        "initial_frozen_long_qty": 0.0,
        "initial_frozen_short_qty": 0.0,
        "drain_state": terminal_drain_state_to_dict(drain_state),
        "intents": [],
        "active_intent_ids": [],
    }
    owner["owner_integrity_digest"] = _terminal_drain_owner_integrity_digest(owner)
    owner["runtime_integrity_digest"] = _terminal_drain_runtime_integrity_digest(owner)
    return owner


def _terminal_intent(
    control: dict[str, object],
    *,
    status: str = "pending",
) -> dict[str, object]:
    snapshot = run_contract_snapshot_from_config(control)
    contract_id = run_contract_identity_from_config(snapshot)
    symbol = str(snapshot["symbol"])
    source = "competition_target_gate"
    trigger_reason = "target_reached"
    requested_at = "2026-07-16T01:00:00+00:00"
    target = float(snapshot["max_cumulative_notional"])
    return {
        "schema": "futures_lifecycle_intent_v2",
        "intent_id": terminal_intent_id(
            symbol=symbol,
            source=source,
            trigger_reason=trigger_reason,
            run_contract_id=contract_id,
        ),
        "symbol": symbol,
        "source": source,
        "action": "lifecycle_drain",
        "trigger_reason": trigger_reason,
        "requested_at": requested_at,
        "status": status,
        "exit_policy": "use_immutable_run_contract",
        "run_contract_id": contract_id,
        "run_contract_snapshot": snapshot,
        "observed": {
            "gross_notional": target + 1.0,
            "target": target,
            "realized_pnl": 0.0,
            "wear_per_10k": 0.0,
            "trade_count": 1,
            "window_start": snapshot["runtime_guard_stats_start_time"],
            "window_end": snapshot["run_end_time"],
            "query_end": requested_at,
        },
    }


def test_default_update_wrapper_installs_real_script_instead_of_self_recursing() -> None:
    script = Path("deploy/oracle/install_or_update.sh").read_text(encoding="utf-8")

    assert 'if [ "${UPDATE_WRAPPER_PATH}" = "/usr/local/bin/grid-web-update" ]' in script
    assert 'install -m 755 "${APP_DIR}/deploy/oracle/grid-web-update.sh" "${UPDATE_WRAPPER_PATH}"' in script


def test_saved_runner_stop_cancels_spot_strategy_orders_before_systemd_stop() -> None:
    script = Path("deploy/oracle/manage_saved_runner.sh").read_text(encoding="utf-8")

    assert "cancel_spot_strategy_orders_if_configured" in script
    assert 'control_path="$APP_DIR/output/${SYMBOL_SLUG}_spot_loop_runner_control.json"' in script
    assert 'if [ ! -f "$control_path" ]; then' in script
    assert "load_exchange_env" in script
    assert "_cancel_spot_strategy_orders(config)" in script
    assert (
        "cancel_spot_strategy_orders_if_configured\n"
        '      run_systemctl stop "$service"\n'
        "      cancel_spot_strategy_orders_if_configured"
    ) in script
    assert "cancel_spot_strategy_orders_if_configured\n      run_systemctl restart" in script


def test_saved_runner_fallback_stop_cancels_spot_strategy_orders_around_process_kill() -> None:
    script = Path("deploy/oracle/manage_saved_runner.sh").read_text(encoding="utf-8")

    assert (
        "stop_runner() {\n"
        "  local pids\n"
        "  cancel_spot_strategy_orders_if_configured"
    ) in script
    assert (
        '    rm -f "$PID_PATH"\n'
        "  fi\n"
        "  cancel_spot_strategy_orders_if_configured\n"
        "}"
    ) in script


def test_runner_systemd_does_not_restart_on_controlled_gate_reject() -> None:
    script = Path("deploy/oracle/install_runner_systemd.sh").read_text(encoding="utf-8")

    assert "Restart=always" in script
    assert "RestartPreventExitStatus=2" in script


def test_disk_pressure_watchdog_has_safe_cleanup_and_alerts() -> None:
    script = Path("deploy/oracle/disk_pressure_watchdog.sh").read_text(encoding="utf-8")

    assert "MAX_ROOT_USE_PCT" in script
    assert "CRITICAL_ROOT_USE_PCT" in script
    assert "cleanup_output_jsonl" in script
    assert "journalctl --vacuum-size" in script
    assert "docker image prune" in script
    assert "send_alert_email" in script
    assert "*.jsonl.gz" in script


def test_disk_pressure_installer_wires_timer_environment() -> None:
    script = Path("deploy/oracle/install_disk_pressure_watchdog.sh").read_text(encoding="utf-8")

    assert "disk_pressure_watchdog.sh" in script
    assert "OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}" in script
    assert "Environment=MAX_ROOT_USE_PCT=${MAX_ROOT_USE_PCT}" in script
    assert "Environment=MAX_OUTPUT_SIZE_MB=${MAX_OUTPUT_SIZE_MB}" in script
    assert "Environment=ALERT_EMAIL_TO=${ALERT_EMAIL_TO}" in script


def test_watchdog_installers_execute_scripts_via_bash() -> None:
    installers = [
        (
            "deploy/oracle/install_web_watchdog.sh",
            "web_health_watchdog.sh",
        ),
        (
            "deploy/oracle/install_host_pressure_watchdog.sh",
            "host_pressure_watchdog.sh",
        ),
        (
            "deploy/oracle/install_disk_pressure_watchdog.sh",
            "disk_pressure_watchdog.sh",
        ),
    ]

    for installer_path, script_name in installers:
        installer = Path(installer_path).read_text(encoding="utf-8")
        assert f"ExecStart=/usr/bin/env bash ${{RUNNER_CODE_DIR}}/deploy/oracle/{script_name}" in installer


def test_watchdogs_do_not_mark_systemd_failed_before_threshold() -> None:
    for script_path in [
        "deploy/oracle/web_health_watchdog.sh",
        "deploy/oracle/host_pressure_watchdog.sh",
        "deploy/oracle/disk_pressure_watchdog.sh",
    ]:
        script = Path(script_path).read_text(encoding="utf-8")
        assert 'if [ "${fail_count}" -lt "${FAILURE_THRESHOLD}" ]; then\n  exit 0\nfi' in script


def test_output_logrotate_installer_uses_copytruncate_timer() -> None:
    script = Path("deploy/oracle/install_output_logrotate.sh").read_text(encoding="utf-8")

    assert "*_loop_events.jsonl" in script
    assert "*_loop_plan_audit.jsonl" in script
    assert "copytruncate" in script
    assert "compress" in script
    assert "delaycompress" not in script
    assert "OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}" in script
    assert "logrotate -s" in script


def test_alpha_airdrop_monitor_installer_writes_expected_systemd_units() -> None:
    script = Path("deploy/oracle/install_alpha_airdrop_monitor.sh").read_text(encoding="utf-8")

    assert 'Description=Monitor Binance Alpha airdrop posts on X' in script
    assert 'ExecStart=${PYTHON_BIN} -m grid_optimizer.alpha_airdrop_monitor' in script
    assert '--accounts ${ACCOUNTS}' in script
    assert '--state-path ${STATE_PATH}' in script
    assert '--alert-config-path ${ALERT_CONFIG_PATH}' in script
    assert 'OnCalendar=*-*-* 14..22:0/10:00' in script
    assert 'Persistent=false' in script
    assert 'sudo systemctl restart "${TIMER_UNIT_NAME}.timer"' in script
    assert 'sudo systemctl start "${TIMER_UNIT_NAME}.service"' not in script


def test_recovery_stall_monitor_installer_runs_every_minute() -> None:
    script = Path("deploy/oracle/install_recovery_stall_monitor.sh").read_text(encoding="utf-8")

    assert "grid_optimizer.recovery_stall_monitor" in script
    assert "--threshold-seconds ${THRESHOLD_SECONDS}" in script
    assert "ON_UNIT_ACTIVE_SEC=\"${ON_UNIT_ACTIVE_SEC:-1min}\"" in script
    assert "sudo systemctl restart \"${TIMER_UNIT_NAME}.timer\"" in script


def test_bq_volume_recovery_guard_oneshot_finishes_before_next_timer_round() -> None:
    script = Path("deploy/oracle/install_bq_volume_recovery_guard.sh").read_text(
        encoding="utf-8"
    )

    assert 'ON_UNIT_ACTIVE_SEC="${ON_UNIT_ACTIVE_SEC:-1min}"' in script
    assert 'TIMEOUT_START_SEC="${TIMEOUT_START_SEC:-45s}"' in script
    assert "Type=oneshot" in script
    assert "TimeoutStartSec=${TIMEOUT_START_SEC}" in script
    assert "OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}" in script
    assert "OnFailure=${FAILURE_ALERT_UNIT_NAME}.service" in script
    assert "grid_optimizer.recovery_coordinator_watchdog" in script
    assert "WATCHDOG_MAX_HEARTBEAT_AGE_SECONDS" in script
    assert 'sudo systemctl enable "${WATCHDOG_UNIT_NAME}.timer"' in script


def test_low_volume_monitor_installer_runs_every_ten_minutes() -> None:
    script = Path("deploy/oracle/install_low_volume_monitor.sh").read_text(encoding="utf-8")

    assert "grid_optimizer.low_volume_monitor" in script
    assert "--min-volume-notional ${MIN_VOLUME_NOTIONAL}" in script
    assert "MIN_VOLUME_NOTIONAL=\"${MIN_VOLUME_NOTIONAL:-1000}\"" in script
    assert "ON_UNIT_ACTIVE_SEC=\"${ON_UNIT_ACTIVE_SEC:-10min}\"" in script
    assert 'status "${TIMER_UNIT_NAME}.service" | sed -n \'1,20p\' || true' in script


def test_runner_watchdog_never_revives_an_intended_stop() -> None:
    """Every restart-capable branch (inactive start, missing events, stale events)
    must consult intended_stop_reason() -- a guard-stopped runner can present as
    inactive OR as active-with-stale-events, and blind revival of either is the
    2026-07-04 ARX incident class."""
    script = Path("deploy/oracle/runner_watchdog.sh").read_text(encoding="utf-8")

    assert "intended_stop_reason() {" in script
    assert "classify_terminal_intent() {" in script
    assert "_terminal_intent.json" in script
    assert "validate_terminal_intent" in script
    assert "grid_optimizer.futures_terminal_ownership" in script
    assert "run_contract_snapshot_from_config" in script
    assert "TERMINAL_INTENT_ACTIVE_STATUSES" in script
    assert "TERMINAL_INTENT_COMPLETED_STATUSES" in script
    assert "run_contract_id" in script
    assert '"$SYMBOL" "$TERMINAL_INTENT_PATH" "$CONTROL_PATH"' in script
    assert "terminal_lifecycle_intent" in script
    assert "inactive with active terminal owner; start/resume" in script
    assert "active terminal owner missing events; restart/resume" in script
    assert "active terminal owner stale; restart/resume" in script
    assert '"stop_reason": *"[^"]+"' in script
    assert "skip start" in script
    assert "skip restart" in script
    # The stale-events guard must run BEFORE the final unconditional restart.
    stale_guard = script.index("events stale (${age}s) but intended stop")
    final_restart = script.rindex('systemctl restart "$SERVICE_NAME"')
    assert stale_guard < final_restart
    # The inactive-start knob stays available.
    assert "RUNNER_WATCHDOG_START_INACTIVE" in script
    assert "recovery_control_owner" in script
    assert "recovery-managed" in script


def test_runner_watchdog_registered_active_terminal_owner_resumes_runner(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _owned_control(
        {
            "symbol": "BCHUSDT",
            "strategy_profile": "test_profile",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "per_order_notional": 20.0,
            "run_start_time": "2026-07-16T00:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
            "run_end_time": "2026-07-17T00:00:00+00:00",
            "max_cumulative_notional": 20_000.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 2.0,
            "terminal_drain_max_wait_seconds": 600.0,
        }
    )
    control = _registered_control(control)
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    control_path.write_text(json.dumps(control), encoding="utf-8")
    snapshot = run_contract_snapshot_from_config(control)
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(_terminal_intent(control)),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 3; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    (fake_bin / "python3").symlink_to(sys.executable)

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    calls = calls_path.read_text(encoding="utf-8") if calls_path.exists() else ""
    assert "start grid-loop@BCHUSDT.service" in calls
    assert "restart grid-loop@BCHUSDT.service" not in calls
    assert "inactive with active terminal owner; start/resume" in completed.stdout
    assert json.loads(control_path.read_text(encoding="utf-8")) == control

    (output_dir / "bchusdt_terminal_intent.json").unlink()
    (output_dir / "bchusdt_loop_state.json").write_text(
        json.dumps(
            {
                "futures_terminal_drain": _terminal_runtime_owner(
                    control,
                    exit_status="exiting",
                )
            }
        ),
        encoding="utf-8",
    )
    calls_path.unlink()
    runtime_owner_resume = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert runtime_owner_resume.returncode == 0, runtime_owner_resume.stderr
    assert "start grid-loop@BCHUSDT.service" in calls_path.read_text(
        encoding="utf-8"
    )
    assert "inactive with active terminal owner; start/resume" in (
        runtime_owner_resume.stdout
    )


def test_runner_watchdog_resumes_valid_terminal_owner_when_control_is_unreadable(
    tmp_path: Path,
) -> None:
    """A frozen terminal contract must outlive a later control-file fault."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _owned_control(
        {
            "symbol": "BCHUSDT",
            "strategy_profile": "test_profile",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "per_order_notional": 20.0,
            "run_start_time": "2026-07-16T00:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
            "run_end_time": "2026-07-17T00:00:00+00:00",
            "max_cumulative_notional": 20_000.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 2.0,
            "terminal_drain_max_wait_seconds": 600.0,
        }
    )
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    control_path.write_text(json.dumps(control), encoding="utf-8")
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(_terminal_intent(control)),
        encoding="utf-8",
    )
    control_path.write_text("{unreadable", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 3; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    (fake_bin / "python3").symlink_to(sys.executable)

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "start grid-loop@BCHUSDT.service" in calls_path.read_text(
        encoding="utf-8"
    )
    assert "inactive with active terminal owner; start/resume" in completed.stdout
    assert control_path.read_text(encoding="utf-8") == "{unreadable"

    (output_dir / "bchusdt_terminal_intent.json").unlink()
    (output_dir / "bchusdt_loop_state.json").write_text(
        json.dumps(
            {
                "futures_terminal_drain": _terminal_runtime_owner(
                    control,
                    exit_status="exiting",
                )
            }
        ),
        encoding="utf-8",
    )
    calls_path.unlink()
    runtime_owner_resume = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert runtime_owner_resume.returncode == 0, runtime_owner_resume.stderr
    assert "start grid-loop@BCHUSDT.service" in calls_path.read_text(
        encoding="utf-8"
    )
    assert "inactive with active terminal owner; start/resume" in (
        runtime_owner_resume.stdout
    )


def test_runner_watchdog_registered_without_terminal_owner_never_actuates(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _owned_control(
        {
            "symbol": "BCHUSDT",
            "strategy_profile": "test_profile",
            "strategy_mode": "hedge_best_quote_maker_volume_v1",
            "per_order_notional": 20.0,
            "run_start_time": "2026-07-16T00:00:00+00:00",
            "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
            "run_end_time": "2026-07-17T00:00:00+00:00",
            "max_cumulative_notional": 20_000.0,
            "terminal_drain_exit_policy": "drain_then_preserve",
            "terminal_drain_absolute_loss_budget": 2.0,
            "terminal_drain_max_wait_seconds": 600.0,
        }
    )
    control = _registered_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 3; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    (fake_bin / "python3").symlink_to(sys.executable)

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    calls = calls_path.read_text(encoding="utf-8") if calls_path.exists() else ""
    assert "start grid-loop@BCHUSDT.service" not in calls
    assert "restart grid-loop@BCHUSDT.service" not in calls
    assert "BCHUSDT is recovery-managed; skip actuator" in completed.stdout


def test_runner_watchdog_mirror_only_registration_waits_for_local_repair(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _registered_control(
        _owned_control(
            {
                "symbol": "BCHUSDT",
                "strategy_profile": "test_profile",
                "run_start_time": "2026-07-16T00:00:00+00:00",
                "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
                "run_end_time": "2026-07-17T00:00:00+00:00",
                "max_cumulative_notional": 20_000.0,
                "terminal_drain_exit_policy": "drain_then_preserve",
                "terminal_drain_absolute_loss_budget": 2.0,
                "terminal_drain_max_wait_seconds": 600.0,
            }
        )
    )
    del control[RECOVERY_STATE_KEY]
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert not calls_path.exists()
    assert "recovery state repair pending; fail closed" in completed.stderr


def test_runner_watchdog_allows_new_run_after_previous_completed_intent(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control: dict[str, object] = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control),
        encoding="utf-8",
    )
    old_control = {**control, "strategy_profile": "old_profile"}
    old_snapshot = run_contract_snapshot_from_config(old_control)
    old_contract_id = run_contract_identity_from_config(old_snapshot)
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(_terminal_intent(old_control, status="stopped_preserved")),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 3; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
    }

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    calls = calls_path.read_text(encoding="utf-8")
    assert "start grid-loop@BCHUSDT.service" in calls
    current_contract_id = run_contract_identity_from_config(control)
    assert current_contract_id != old_contract_id

    current_snapshot = run_contract_snapshot_from_config(control)
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(_terminal_intent(control, status="stopped_clean")),
        encoding="utf-8",
    )
    calls_path.unlink()

    same_run = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert same_run.returncode == 0, same_run.stderr
    same_run_calls = calls_path.read_text(encoding="utf-8")
    assert "start grid-loop@BCHUSDT.service" not in same_run_calls
    assert "intended stop (terminal_lifecycle_intent); skip start" in same_run.stdout


def test_runner_watchdog_fails_closed_before_systemctl_when_bounded_owner_missing(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    raw_control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(raw_control), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert not calls_path.exists()
    assert "bounded run contract owner is missing" in completed.stderr


def test_runner_watchdog_resumes_active_terminal_owner_even_when_managed_and_start_disabled(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "ARXUSDT",
        "strategy_profile": "test_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    (output_dir / "arxusdt_loop_runner_control.json").write_text(
        json.dumps(control),
        encoding="utf-8",
    )
    snapshot = run_contract_snapshot_from_config(control)
    contract_id = run_contract_identity_from_config(snapshot)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 3; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
        "RUNNER_WATCHDOG_START_INACTIVE": "0",
    }

    for status in ("pending", "accepted", "executing", "exit_blocked"):
        (output_dir / "arxusdt_terminal_intent.json").write_text(
            json.dumps(_terminal_intent(control, status=status)),
            encoding="utf-8",
        )
        calls_path.unlink(missing_ok=True)

        completed = subprocess.run(
            ["bash", "deploy/oracle/runner_watchdog.sh", "ARXUSDT"],
            cwd=Path.cwd(),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        calls = calls_path.read_text(encoding="utf-8")
        assert "start grid-loop@ARXUSDT.service" in calls
        assert "inactive with active terminal owner; start/resume" in completed.stdout


def test_runner_watchdog_restarts_active_terminal_owner_when_events_missing(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    control = _registered_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(json.dumps(control), encoding="utf-8")
    snapshot = run_contract_snapshot_from_config(control)
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(_terminal_intent(control)),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 0; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
    }

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "restart grid-loop@BCHUSDT.service" in calls_path.read_text(encoding="utf-8")
    assert "active terminal owner missing events; restart/resume" in completed.stdout


@pytest.mark.parametrize(
    ("exit_status", "owner_is_current", "expect_start"),
    (
        ("exiting", True, True),
        ("exit_blocked", False, True),
        ("stopped_clean", True, False),
        ("stopped_preserved", False, True),
    ),
)
def test_runner_watchdog_uses_internal_terminal_owner_before_old_stop_event(
    tmp_path: Path,
    exit_status: str,
    owner_is_current: bool,
    expect_start: bool,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "new_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control),
        encoding="utf-8",
    )
    owner_control = (
        control
        if owner_is_current
        else {**control, "strategy_profile": "old_profile"}
    )
    runtime_owner = _terminal_runtime_owner(
        owner_control,
        exit_status=exit_status,
    )
    (output_dir / "bchusdt_loop_state.json").write_text(
        json.dumps(
            {
                "futures_terminal_drain": runtime_owner,
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "bchusdt_loop_events.jsonl").write_text(
        '{"stop_reason":"max_cumulative_notional_hit"}\n',
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 3; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
    }

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    calls = calls_path.read_text(encoding="utf-8")
    assert ("start grid-loop@BCHUSDT.service" in calls) is expect_start
    if exit_status in {"exiting", "exit_blocked"}:
        assert "inactive with active terminal owner; start/resume" in completed.stdout
    elif owner_is_current:
        assert "intended stop (terminal_lifecycle_intent); skip start" in completed.stdout


def test_runner_watchdog_restarts_stale_internal_terminal_owner(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    runtime_owner = _terminal_runtime_owner(control, exit_status="exiting")
    (output_dir / "bchusdt_loop_state.json").write_text(
        json.dumps(
            {
                "futures_terminal_drain": runtime_owner,
            }
        ),
        encoding="utf-8",
    )
    events_path = output_dir / "bchusdt_loop_events.jsonl"
    events_path.write_text('{"stop_reason":"after_end_window"}\n', encoding="utf-8")
    os.utime(events_path, (1, 1))
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then exit 0; fi\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    fake_stat = fake_bin / "stat"
    fake_stat.write_text("#!/bin/sh\necho 1\n", encoding="utf-8")
    fake_stat.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
        "RUNNER_WATCHDOG_STALE_SECONDS": "1",
    }

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "restart grid-loop@BCHUSDT.service" in calls_path.read_text(encoding="utf-8")
    assert "active terminal owner stale; restart/resume" in completed.stdout


@pytest.mark.parametrize(
    ("service_active", "handoff_status", "expected_action", "expected_message"),
    (
        (
            False,
            "pending",
            "start",
            "inactive with terminal handoff pending; start/resume",
        ),
        (
            True,
            "pending",
            "restart",
            "terminal handoff pending and events stale; restart/resume",
        ),
        (
            True,
            "acknowledged",
            None,
            "events stale",
        ),
    ),
)
def test_runner_watchdog_uses_durable_terminal_handoff_state(
    tmp_path: Path,
    service_active: bool,
    handoff_status: str,
    expected_action: str | None,
    expected_message: str,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "new_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    current_contract_id = run_contract_identity_from_config(control)
    old_control = {**control, "strategy_profile": "old_profile"}
    old_snapshot = run_contract_snapshot_from_config(old_control)
    old_contract_id = run_contract_identity_from_config(old_snapshot)
    (output_dir / "bchusdt_loop_state.json").write_text(
        json.dumps(
            {
                "futures_terminal_handoff": {
                    "schema": "futures_terminal_handoff_v1",
                    "symbol": "BCHUSDT",
                    "status": handoff_status,
                    "from_decision_id": f"BCHUSDT|{old_contract_id}",
                    "to_decision_id": f"BCHUSDT|{current_contract_id}",
                    "run_contract_id": (
                        "damaged-old-id"
                        if handoff_status == "acknowledged"
                        else old_contract_id
                    ),
                    "run_contract_snapshot": (
                        {"damaged": True}
                        if handoff_status == "acknowledged"
                        else old_snapshot
                    ),
                    "created_at": "2026-07-16T00:01:00+00:00",
                    "acknowledged_at": (
                        "2026-07-16T00:02:00+00:00"
                        if handoff_status == "acknowledged"
                        else None
                    ),
                }
            }
        ),
        encoding="utf-8",
    )
    events_path = output_dir / "bchusdt_loop_events.jsonl"
    events_path.write_text(
        '{"stop_reason":"max_cumulative_notional_hit"}\n', encoding="utf-8"
    )
    os.utime(events_path, (1, 1))
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        'if [ "$1" = "is-active" ]; then '
        f"exit {0 if service_active else 3}; fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    fake_stat = fake_bin / "stat"
    fake_stat.write_text("#!/bin/sh\necho 1\n", encoding="utf-8")
    fake_stat.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
        "RUNNER_WATCHDOG_STALE_SECONDS": "1",
    }

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    calls = calls_path.read_text(encoding="utf-8")
    if expected_action is None:
        assert "start grid-loop@BCHUSDT.service" not in calls
        assert "restart grid-loop@BCHUSDT.service" not in calls
    else:
        assert f"{expected_action} grid-loop@BCHUSDT.service" in calls
    assert expected_message in completed.stdout


def test_runner_watchdog_fails_closed_on_tampered_terminal_snapshot(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(json.dumps(control), encoding="utf-8")
    snapshot = run_contract_snapshot_from_config(control)
    contract_id = run_contract_identity_from_config(snapshot)
    snapshot["terminal_drain_absolute_loss_budget"] = 200.0
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(
            {
                "schema": "futures_lifecycle_intent_v2",
                "intent_id": "BCHUSDT-tampered",
                "symbol": "BCHUSDT",
                "action": "lifecycle_drain",
                "status": "pending",
                "run_contract_id": contract_id,
                "run_contract_snapshot": snapshot,
            }
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    env = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "SYSTEMCTL_CALLS": str(calls_path),
    }

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert not calls_path.exists()
    assert "fail closed" in completed.stderr


@pytest.mark.parametrize(
    "proof_case",
    ("missing_observed", "fake_target", "fake_deadline", "fake_wear"),
)
def test_runner_watchdog_rejects_invalid_terminal_proof_before_systemctl(
    tmp_path: Path,
    proof_case: str,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control: dict[str, object] = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    if proof_case == "fake_wear":
        control.update(
            lifecycle_wear_stop_per_10k=2.0,
            lifecycle_wear_stop_min_gross_notional=75_000.0,
        )
    control = _owned_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control),
        encoding="utf-8",
    )
    intent = _terminal_intent(control)
    observed = intent["observed"]
    if proof_case == "missing_observed":
        intent.pop("observed")
    elif proof_case == "fake_target":
        observed["gross_notional"] = 19_999.0
    elif proof_case == "fake_deadline":
        intent["trigger_reason"] = "target_unmet_deadline"
        intent["requested_at"] = "2026-07-17T00:00:01+00:00"
        observed.update(
            gross_notional=19_999.0,
            query_end="2026-07-17T00:00:00+00:00",
            runtime_guard_primary_reason="max_actual_net_notional_hit",
            runtime_guard_matched_reasons=["after_end_window"],
        )
    else:
        intent["trigger_reason"] = "wear_limit_breached"
        observed.update(
            gross_notional=80_000.0,
            realized_pnl=-8.0,
            wear_per_10k=1.0,
            first=75_000.0,
            wear_stop=2.0,
        )
    if proof_case in {"fake_deadline", "fake_wear"}:
        intent["intent_id"] = terminal_intent_id(
            symbol="BCHUSDT",
            source="competition_target_gate",
            trigger_reason=str(intent["trigger_reason"]),
            run_contract_id=str(intent["run_contract_id"]),
        )
    (output_dir / "bchusdt_terminal_intent.json").write_text(
        json.dumps(intent),
        encoding="utf-8",
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert not calls_path.exists()
    assert "terminal intent is invalid; fail closed" in completed.stderr


def test_runner_watchdog_fails_closed_on_tampered_terminal_runtime_digest(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = {
        "symbol": "BCHUSDT",
        "strategy_profile": "test_profile",
        "strategy_mode": "hedge_best_quote_maker_volume_v1",
        "per_order_notional": 20.0,
        "run_start_time": "2026-07-16T00:00:00+00:00",
        "runtime_guard_stats_start_time": "2026-07-16T00:00:00+00:00",
        "run_end_time": "2026-07-17T00:00:00+00:00",
        "max_cumulative_notional": 20_000.0,
        "terminal_drain_exit_policy": "drain_then_preserve",
        "terminal_drain_absolute_loss_budget": 2.0,
        "terminal_drain_max_wait_seconds": 600.0,
    }
    control = _owned_control(control)
    control = _registered_control(control)
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control),
        encoding="utf-8",
    )
    owner = _terminal_runtime_owner(control, exit_status="stopped_preserved")
    owner["runtime_integrity_digest"] = "tampered"
    (output_dir / "bchusdt_loop_state.json").write_text(
        json.dumps({"futures_terminal_drain": owner}),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$SYSTEMCTL_CALLS"\n'
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    completed = subprocess.run(
        ["bash", "deploy/oracle/runner_watchdog.sh", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "SYSTEMCTL_CALLS": str(calls_path),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert not calls_path.exists()
    assert "terminal runtime owner integrity mismatch" in completed.stderr
    assert "fail closed" in completed.stderr


def test_single_writer_installer_disables_only_legacy_arx_o_actuators() -> None:
    script = Path("deploy/oracle/install_recovery_single_writer.sh").read_text(encoding="utf-8")

    assert "arxusdt_ledger_drift_monitor\\.py" in script
    assert "ousdt_ledger_drift_monitor\\.py" in script
    assert "rollover_daily_window\\.py" in script
    assert "--symbols OUSDT,ARXUSDT" in script
    assert "recovery-single-writer disabled" in script
    assert "crontab -" in script


def test_runner_systemd_installer_narrows_restart_to_failures() -> None:
    """Runtime-guard stops exit cleanly; Restart=always would blind-revive a
    risk-stopped runner. The installer must ship the on-failure drop-in that
    production host 150 already carries."""
    script = Path("deploy/oracle/install_runner_systemd.sh").read_text(encoding="utf-8")

    assert "80-runtime-guard-stop.conf" in script
    assert "Restart=on-failure" in script


def test_recovery_managed_runner_policy_requires_registered_coordinator_owner() -> None:
    script = Path(
        "deploy/oracle/configure_recovery_managed_runner.sh"
    ).read_text(encoding="utf-8")

    assert "audit|enable|retire|disable|verify" in script
    assert "recovery_coordinator_registered" in script
    assert "decode_recovery_control_state" in script
    assert 'Restart=no' in script
    assert 'systemctl is-active --quiet "${COORDINATOR_TIMER_UNIT}.timer"' in script
    assert "require_coordinator_heartbeat" in script
    assert "require_coordinator_watchdog_timer" in script
    assert "require_coordinator_watchdog_heartbeat" in script
    assert "require_coordinator_alert_delivery" in script
    assert "require_last_valid_control_snapshot" in script
    assert "require_target_gate_scheduler" in script
    assert "control_last_valid_snapshot_path" in script
    assert "load_alert_notifier_config" in script
    assert "recovery_coordinator_watchdog_heartbeat_v1" in script
    assert "futures_recovery_guard_heartbeat_v1" in script
    assert "registered control must be retired before disabling" in script
    assert "require_no_legacy_recovery_actuators" in script
    assert "ledger_drift_monitor.py" in script
    assert "allowloss_autorevert.py" in script
    assert "rollover_daily_window.py" in script
    assert "list-timers" in script
    assert "list-units" in script
    assert "active_services" in script
    assert "ExecStart" in script
    assert "unclassified systemd recovery executors" in script
    assert 'systemctl daemon-reload' in script


def test_recovery_managed_runner_policy_enables_only_registered_symbol(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _registered_control({"symbol": "BCHUSDT"})
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    (output_dir / "bchusdt_loop_runner_control.json.last_valid").write_text(
        json.dumps(control), encoding="utf-8"
    )
    (output_dir / "bq_volume_recovery_guard_state.json").write_text(
        json.dumps(
            {
                "futures_recovery_guard_heartbeat": {
                    "schema": "futures_recovery_guard_heartbeat_v1",
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                    "ok": True,
                    "symbols": {
                        "BCHUSDT": {
                            "healthy": True,
                            "action": "coordinator_noop_hold",
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "recovery_coordinator_watchdog_state.json").write_text(
        json.dumps(
            {
                "recovery_coordinator_watchdog_heartbeat": {
                    "schema": "recovery_coordinator_watchdog_heartbeat_v1",
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                    "ok": True,
                    "symbols": {"BCHUSDT": True},
                }
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "alert_notifier_config.json").write_text(
        json.dumps({"email_to": ["ops@example.com"]}), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls_path = tmp_path / "systemctl.calls"
    (fake_bin / "sudo").write_text(
        "#!/bin/sh\nexec \"$@\"\n", encoding="utf-8"
    )
    (fake_bin / "crontab").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = -l ]; then\n"
        "  echo '*/5 * * * * python -m grid_optimizer.competition_target_gate --symbol BCHUSDT --enforce'\n"
        "fi\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        f"echo \"$@\" >> {calls_path}\n"
        "if [ \"$1\" = is-active ]; then exit 0; fi\n"
        "if [ \"$1\" = show ]; then echo no; exit 0; fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    (fake_bin / "sudo").chmod(0o755)
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        [
            "bash",
            "deploy/oracle/configure_recovery_managed_runner.sh",
            "enable",
            "BCHUSDT",
        ],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "SYSTEMD_DIR": str(tmp_path / "systemd"),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    dropin = (
        tmp_path
        / "systemd"
        / "grid-loop@BCHUSDT.service.d"
        / "90-futures-recovery-managed.conf"
    )
    assert dropin.read_text(encoding="utf-8") == "[Service]\nRestart=no\n"
    calls = calls_path.read_text(encoding="utf-8")
    assert "is-active --quiet grid-bq-volume-recovery-guard.timer" in calls
    assert "daemon-reload" in calls
    assert "start grid-loop@BCHUSDT.service" not in calls
    assert "restart grid-loop@BCHUSDT.service" not in calls


@pytest.mark.parametrize(
    ("snapshot_text", "target_gate_mode", "error"),
    [
        (None, "cron", "last-valid recovery control snapshot is not valid"),
        (
            json.dumps({"symbol": "BCHUSDT"}),
            "cron",
            "last-valid recovery control snapshot is not valid",
        ),
        (None, "missing", "no active target-gate scheduler found"),
        (None, "inactive_timer", "no active target-gate scheduler found"),
    ],
)
def test_recovery_managed_runner_policy_refuses_enable_without_valid_recovery_snapshot(
    tmp_path: Path,
    snapshot_text: str | None,
    target_gate_mode: str,
    error: str,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _registered_control({"symbol": "BCHUSDT"})
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    if snapshot_text is not None:
        (output_dir / "bchusdt_loop_runner_control.json.last_valid").write_text(
            snapshot_text,
            encoding="utf-8",
        )
    now = datetime.now(timezone.utc).isoformat()
    (output_dir / "bq_volume_recovery_guard_state.json").write_text(
        json.dumps(
            {
                "futures_recovery_guard_heartbeat": {
                    "schema": "futures_recovery_guard_heartbeat_v1",
                    "checked_at": now,
                    "ok": True,
                    "symbols": {"BCHUSDT": {"healthy": True}},
                }
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "recovery_coordinator_watchdog_state.json").write_text(
        json.dumps(
            {
                "recovery_coordinator_watchdog_heartbeat": {
                    "schema": "recovery_coordinator_watchdog_heartbeat_v1",
                    "checked_at": now,
                    "ok": True,
                    "symbols": {"BCHUSDT": True},
                }
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "alert_notifier_config.json").write_text(
        json.dumps({"email_to": ["ops@example.com"]}), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "sudo").write_text("#!/bin/sh\nexec \"$@\"\n", encoding="utf-8")
    (fake_bin / "crontab").write_text(
        (
            "#!/bin/sh\n"
            "if [ \"$1\" = -l ]; then\n"
            "  echo '*/5 * * * * python -m grid_optimizer.competition_target_gate --symbol BCHUSDT --enforce'\n"
            "fi\n"
            if target_gate_mode == "cron"
            else "#!/bin/sh\nexit 0\n"
        ),
        encoding="utf-8",
    )
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        + (
            "if [ \"$1\" = list-timers ]; then echo 'n/a n/a n/a n/a n/a target-gate.timer'; exit 0; fi\n"
            "if [ \"$1\" = show ] && [ \"$3\" = Unit ]; then echo target-gate.service; exit 0; fi\n"
            "if [ \"$1\" = show ] && [ \"$3\" = ExecStart ]; then echo 'python -m grid_optimizer.competition_target_gate --symbol BCHUSDT --enforce'; exit 0; fi\n"
            "if [ \"$1\" = is-active ] && [ \"$3\" = target-gate.timer ]; then exit 3; fi\n"
            if target_gate_mode == "inactive_timer"
            else ""
        )
        + "if [ \"$1\" = is-active ]; then exit 0; fi\n"
        + "if [ \"$1\" = show ]; then echo no; exit 0; fi\n"
        + "exit 0\n",
        encoding="utf-8",
    )
    (fake_bin / "sudo").chmod(0o755)
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "enable", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "SYSTEMD_DIR": str(tmp_path / "systemd"),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert error in completed.stderr
    assert not (tmp_path / "systemd").exists()


@pytest.mark.parametrize(
    "legacy_writer",
    [
        "python output/ops/bchusdt_ledger_drift_monitor.py --enforce",
        "python output/ops/bchusdt_allowloss_autorevert.py --enforce",
        "python output/ops/rollover_daily_window.py --symbols BCHUSDT,ARXUSDT",
    ],
)
def test_recovery_managed_runner_policy_audit_rejects_a_legacy_symbol_writer(
    tmp_path: Path,
    legacy_writer: str,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = -l ]; then\n"
        f"  echo \"*/10 * * * * {legacy_writer}\"\n"
        "fi\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "legacy recovery actuators remain" in completed.stderr
    assert legacy_writer in completed.stderr


def test_recovery_managed_runner_policy_audit_rejects_an_unclassified_systemd_timer(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text("#!/bin/sh\n", encoding="utf-8")
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "case \"$1 $2 $3\" in\n"
        "  'list-timers --all --no-legend') echo 'n/a n/a n/a n/a legacy-bch.timer' ;;\n"
        "  'show -p Unit') echo legacy-bch.service ;;\n"
        "  'show -p ExecStart') echo 'python output/ops/unknown_recovery.py --symbol BCHUSDT --enforce' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "unclassified systemd recovery executors remain" in completed.stderr
    assert "legacy-bch.timer->legacy-bch.service" in completed.stderr


def test_recovery_managed_runner_policy_audit_rejects_an_unclassified_active_systemd_service(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text("#!/bin/sh\n", encoding="utf-8")
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "case \"$1 $2 $3\" in\n"
        "  'list-timers --all --no-legend') exit 0 ;;\n"
        "  'list-units --type=service --state=active') echo 'legacy-stop-bch.service loaded active running Legacy BCH stop loop' ;;\n"
        "  'show -p ExecStart') echo 'python output/ops/unknown_recovery.py --symbol BCHUSDT --enforce' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "unclassified systemd recovery executors remain" in completed.stderr
    assert "legacy-stop-bch.service" in completed.stderr


def test_recovery_managed_runner_policy_audit_allows_its_managed_runner_service(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text("#!/bin/sh\n", encoding="utf-8")
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "case \"$1 $2 $3\" in\n"
        "  'list-timers --all --no-legend') exit 0 ;;\n"
        "  'list-units --type=service --state=active') echo 'grid-loop@BCHUSDT.service loaded active running Managed runner' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_recovery_managed_runner_policy_audit_allows_target_gate_and_observers(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = -l ]; then\n"
        "  echo \"*/5 * * * * python output/ops/competition_target_gate.py --symbol BCHUSDT --enforce\"\n"
        "  echo \"*/5 * * * * python output/ops/competition_health_monitor.py --symbol BCHUSDT --enforce\"\n"
        "  echo \"*/5 * * * * python output/ops/bq_liveness_terminal_guard.py --symbol BCHUSDT --enforce\"\n"
        "fi\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = list-timers ]; then exit 0; fi\n"
        "if [ \"$1\" = list-units ]; then exit 0; fi\n"
        "exit 1\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "no legacy recovery actuators found: BCHUSDT" in completed.stdout


def test_recovery_managed_runner_policy_audit_rejects_target_gate_direct_tp_mode(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = -l ]; then\n"
        "  echo \"*/5 * * * * python output/ops/competition_target_gate.py --symbol BCHUSDT --place-tp-now --enforce\"\n"
        "fi\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = list-timers ]; then exit 0; fi\n"
        "if [ \"$1\" = list-units ]; then exit 0; fi\n"
        "exit 1\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "unclassified symbol executor" in completed.stderr
    assert "--place-tp-now" in completed.stderr


def test_recovery_managed_runner_policy_audit_refuses_unavailable_systemd_timer_inventory(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text("#!/bin/sh\n", encoding="utf-8")
    (fake_bin / "systemctl").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "cannot verify systemd recovery executors" in completed.stderr
    assert "list_timers" in completed.stderr


def test_recovery_managed_runner_policy_audit_refuses_unreadable_systemd_timer_service(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text("#!/bin/sh\n", encoding="utf-8")
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "case \"$1 $2 $3\" in\n"
        "  'list-timers --all --no-legend') echo 'n/a n/a n/a n/a unknown.timer' ;;\n"
        "  'show -p Unit') exit 1 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "cannot verify systemd recovery executors" in completed.stderr
    assert "unknown.timer" in completed.stderr


def test_recovery_managed_runner_policy_audit_rejects_an_unclassified_symbol_executor(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "crontab").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = -l ]; then\n"
        "  echo \"*/5 * * * * python output/ops/unknown_recovery.py --symbol BCHUSDT --enforce\"\n"
        "fi\n",
        encoding="utf-8",
    )
    (fake_bin / "crontab").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "audit", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "unclassified symbol executor" in completed.stderr
    assert "unknown_recovery.py" in completed.stderr


def test_recovery_managed_runner_policy_refuses_stale_coordinator_heartbeat(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    (output_dir / "bq_volume_recovery_guard_state.json").write_text(
        json.dumps(
            {
                "futures_recovery_guard_heartbeat": {
                    "schema": "futures_recovery_guard_heartbeat_v1",
                    "checked_at": "2026-07-16T00:00:00+00:00",
                    "ok": True,
                    "symbols": {"BCHUSDT": {"healthy": True}},
                }
            }
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = list-timers ]; then exit 0; fi\n"
        "if [ \"$1\" = list-units ]; then exit 0; fi\n"
        "if [ \"$1\" = is-active ]; then exit 0; fi\n"
        "exit 1\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "enable", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "SYSTEMD_DIR": str(tmp_path / "systemd"),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "heartbeat is not fresh" in completed.stderr
    assert not (tmp_path / "systemd").exists()


def test_recovery_managed_runner_policy_refuses_missing_watchdog_heartbeat(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    (output_dir / "bq_volume_recovery_guard_state.json").write_text(
        json.dumps(
            {
                "futures_recovery_guard_heartbeat": {
                    "schema": "futures_recovery_guard_heartbeat_v1",
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                    "ok": True,
                    "symbols": {"BCHUSDT": {"healthy": True}},
                }
            }
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = list-timers ]; then exit 0; fi\n"
        "if [ \"$1\" = list-units ]; then exit 0; fi\n"
        "if [ \"$1\" = is-active ]; then exit 0; fi\n"
        "exit 1\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "enable", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "SYSTEMD_DIR": str(tmp_path / "systemd"),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "watchdog heartbeat is not fresh" in completed.stderr
    assert not (tmp_path / "systemd").exists()


def test_recovery_managed_runner_policy_refuses_unconfigured_alert_delivery(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    now = datetime.now(timezone.utc).isoformat()
    (output_dir / "bq_volume_recovery_guard_state.json").write_text(
        json.dumps(
            {
                "futures_recovery_guard_heartbeat": {
                    "schema": "futures_recovery_guard_heartbeat_v1",
                    "checked_at": now,
                    "ok": True,
                    "symbols": {"BCHUSDT": {"healthy": True}},
                }
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "recovery_coordinator_watchdog_state.json").write_text(
        json.dumps(
            {
                "recovery_coordinator_watchdog_heartbeat": {
                    "schema": "recovery_coordinator_watchdog_heartbeat_v1",
                    "checked_at": now,
                    "ok": True,
                    "symbols": {"BCHUSDT": True},
                }
            }
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = list-timers ]; then exit 0; fi\n"
        "if [ \"$1\" = list-units ]; then exit 0; fi\n"
        "if [ \"$1\" = is-active ]; then exit 0; fi\n"
        "exit 1\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "enable", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "SYSTEMD_DIR": str(tmp_path / "systemd"),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "alert delivery is not configured" in completed.stderr
    assert not (tmp_path / "systemd").exists()


def test_recovery_managed_runner_policy_refuses_rollback_while_registered(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control = _registered_control({"symbol": "BCHUSDT"})
    (output_dir / "bchusdt_loop_runner_control.json").write_text(
        json.dumps(control), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "sudo").write_text(
        "#!/bin/sh\nexec \"$@\"\n", encoding="utf-8"
    )
    (fake_bin / "sudo").chmod(0o755)
    dropin = (
        tmp_path
        / "systemd"
        / "grid-loop@BCHUSDT.service.d"
        / "90-futures-recovery-managed.conf"
    )
    dropin.parent.mkdir(parents=True)
    dropin.write_text("[Service]\nRestart=no\n", encoding="utf-8")

    completed = subprocess.run(
        [
            "bash",
            "deploy/oracle/configure_recovery_managed_runner.sh",
            "disable",
            "BCHUSDT",
        ],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "SYSTEMD_DIR": str(tmp_path / "systemd"),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "registered control must be retired" in completed.stderr
    assert dropin.read_text(encoding="utf-8") == "[Service]\nRestart=no\n"


def test_recovery_managed_runner_policy_retires_only_an_inactive_quiescent_symbol(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    control_path.write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\nif [ \"$1\" = is-active ]; then exit 3; fi\nexit 0\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)

    completed = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "retire", "BCHUSDT"],
        cwd=Path.cwd(),
        env={
            **os.environ,
            "APP_DIR": str(tmp_path),
            "PYTHON_BIN": sys.executable,
            "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
            "OUTPUT_DIR": str(output_dir),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    retired = json.loads(control_path.read_text(encoding="utf-8"))
    assert RECOVERY_STATE_KEY not in retired
    assert RECOVERY_STATE_MIRROR_KEY not in retired
    assert "recovery coordinator ownership retired" in completed.stdout


def test_recovery_managed_runner_policy_disables_only_after_safe_retirement(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    control_path = output_dir / "bchusdt_loop_runner_control.json"
    control_path.write_text(
        json.dumps(_registered_control({"symbol": "BCHUSDT"})), encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "sudo").write_text("#!/bin/sh\nexec \"$@\"\n", encoding="utf-8")
    (fake_bin / "systemctl").write_text(
        "#!/bin/sh\nif [ \"$1\" = is-active ]; then exit 3; fi\nexit 0\n",
        encoding="utf-8",
    )
    (fake_bin / "sudo").chmod(0o755)
    (fake_bin / "systemctl").chmod(0o755)
    dropin = (
        tmp_path
        / "systemd"
        / "grid-loop@BCHUSDT.service.d"
        / "90-futures-recovery-managed.conf"
    )
    dropin.parent.mkdir(parents=True)
    dropin.write_text("[Service]\nRestart=no\n", encoding="utf-8")
    environment = {
        **os.environ,
        "APP_DIR": str(tmp_path),
        "PYTHON_BIN": sys.executable,
        "RUNNER_SRC_DIR": str(Path.cwd() / "src"),
        "OUTPUT_DIR": str(output_dir),
        "SYSTEMD_DIR": str(tmp_path / "systemd"),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
    }

    retired = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "retire", "BCHUSDT"],
        cwd=Path.cwd(), env=environment, capture_output=True, text=True, check=False,
    )
    disabled = subprocess.run(
        ["bash", "deploy/oracle/configure_recovery_managed_runner.sh", "disable", "BCHUSDT"],
        cwd=Path.cwd(), env=environment, capture_output=True, text=True, check=False,
    )

    assert retired.returncode == 0, retired.stderr
    assert disabled.returncode == 0, disabled.stderr
    assert not dropin.exists()
