from __future__ import annotations

from pathlib import Path


def test_default_update_wrapper_installs_real_script_instead_of_self_recursing() -> None:
    script = Path("deploy/oracle/install_or_update.sh").read_text(encoding="utf-8")

    assert 'if [ "${UPDATE_WRAPPER_PATH}" = "/usr/local/bin/grid-web-update" ]' in script
    assert 'install -m 755 "${APP_DIR}/deploy/oracle/grid-web-update.sh" "${UPDATE_WRAPPER_PATH}"' in script


def test_alpha_airdrop_monitor_installer_writes_expected_systemd_units() -> None:
    script = Path("deploy/oracle/install_alpha_airdrop_monitor.sh").read_text(encoding="utf-8")

    assert 'Description=Monitor Binance Alpha airdrop posts on X' in script
    assert 'ExecStart=${PYTHON_BIN} -m grid_optimizer.alpha_airdrop_monitor' in script
    assert '--accounts ${ACCOUNTS}' in script
    assert '--state-path ${STATE_PATH}' in script
    assert '--alert-config-path ${ALERT_CONFIG_PATH}' in script
    assert 'OnUnitActiveSec=10min' in script
    assert 'sudo systemctl restart "${TIMER_UNIT_NAME}.timer"' in script
