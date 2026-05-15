from __future__ import annotations

from pathlib import Path


def test_default_update_wrapper_installs_real_script_instead_of_self_recursing() -> None:
    script = Path("deploy/oracle/install_or_update.sh").read_text(encoding="utf-8")

    assert 'if [ "${UPDATE_WRAPPER_PATH}" = "/usr/local/bin/grid-web-update" ]' in script
    assert 'install -m 755 "${APP_DIR}/deploy/oracle/grid-web-update.sh" "${UPDATE_WRAPPER_PATH}"' in script


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
    assert 'OnUnitActiveSec=10min' in script
    assert 'sudo systemctl restart "${TIMER_UNIT_NAME}.timer"' in script
