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


def test_output_logrotate_installer_uses_copytruncate_timer() -> None:
    script = Path("deploy/oracle/install_output_logrotate.sh").read_text(encoding="utf-8")

    assert "*_loop_events.jsonl" in script
    assert "*_loop_plan_audit.jsonl" in script
    assert "copytruncate" in script
    assert "compress" in script
    assert "delaycompress" not in script
    assert "OnUnitActiveSec=${ON_UNIT_ACTIVE_SEC}" in script
    assert "logrotate -s" in script
