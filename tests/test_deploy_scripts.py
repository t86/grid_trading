from __future__ import annotations

from pathlib import Path


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
    assert "target_gate_done_" in script
    assert '"stop_reason": *"[^"]+"' in script
    # Three guarded branches: inactive-start, missing-events restart, stale-events restart.
    assert script.count('if stop_reason="$(intended_stop_reason)"; then') == 3
    assert "skip start" in script
    assert "skip restart" in script
    # The stale-events guard must run BEFORE the final unconditional restart.
    stale_guard = script.index("events stale (${age}s) but intended stop")
    final_restart = script.rindex('systemctl restart "$SERVICE_NAME"')
    assert stale_guard < final_restart
    # The inactive-start knob stays available.
    assert "RUNNER_WATCHDOG_START_INACTIVE" in script


def test_runner_systemd_installer_narrows_restart_to_failures() -> None:
    """Runtime-guard stops exit cleanly; Restart=always would blind-revive a
    risk-stopped runner. The installer must ship the on-failure drop-in that
    production host 150 already carries."""
    script = Path("deploy/oracle/install_runner_systemd.sh").read_text(encoding="utf-8")

    assert "80-runtime-guard-stop.conf" in script
    assert "Restart=on-failure" in script
