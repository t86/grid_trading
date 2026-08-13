from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import textwrap

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "deploy/oracle/install_alpha_competition_dashboard.sh"
SERVICE_NAME = "test-alpha-dashboard"


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(0o755)


@pytest.fixture
def installer_env(tmp_path: Path) -> tuple[dict[str, str], Path]:
    app_dir = tmp_path / "app"
    (app_dir / "src/grid_optimizer").mkdir(parents=True)
    python_bin = app_dir / ".venv/bin/python"
    python_bin.parent.mkdir(parents=True)
    _write_executable(python_bin, "#!/bin/sh\nexit 0\n")

    fake_root = tmp_path / "fake-root"
    (fake_root / "systemd").mkdir(parents=True)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_executable(
        fake_bin / "realpath",
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import os
            from pathlib import Path
            import sys

            args = [arg for arg in sys.argv[1:] if arg != "-m"]
            path = Path(args[-1])
            if "-m" not in sys.argv[1:] and not path.exists():
                sys.exit(1)
            print(os.path.realpath(path))
            """
        ),
    )
    _write_executable(
        fake_bin / "chown",
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            from pathlib import Path
            import sys

            Path(os.environ["FAKE_CHOWN_MARKER"]).write_text(
                json.dumps(sys.argv[1:])
            )
            """
        ),
    )
    _write_executable(
        fake_bin / "sudo",
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            from pathlib import Path
            import shutil
            import subprocess
            import sys

            root = Path(os.environ["FAKE_SYSTEM_ROOT"])
            log_path = Path(os.environ["FAKE_SUDO_LOG"])
            args = sys.argv[1:]
            with log_path.open("a") as handle:
                handle.write(json.dumps(args) + "\\n")

            def mapped(raw: str) -> Path:
                prefix = "/etc/systemd/system/"
                if raw.startswith(prefix):
                    return root / "systemd" / raw.removeprefix(prefix)
                return Path(raw)

            command, rest = args[0], args[1:]
            if command == "bash" and rest[:2] == ["-s", "--"]:
                script = sys.stdin.read()
                if os.environ.get("FAKE_RACE_CACHE_ROOT") == "1":
                    cache_root = Path(rest[2]) / ".cache"
                    outside = Path(os.environ["FAKE_RACE_OUTSIDE"])
                    outside.mkdir(parents=True, exist_ok=True)
                    if cache_root.is_dir() and not cache_root.is_symlink():
                        shutil.rmtree(cache_root)
                    else:
                        cache_root.unlink(missing_ok=True)
                    cache_root.symlink_to(outside, target_is_directory=True)
                completed = subprocess.run(
                    ["/bin/bash", "-s", "--", *rest[2:]],
                    input=script,
                    text=True,
                    env=os.environ,
                )
                sys.exit(completed.returncode)

            if command == "-u" and rest[1:3] == ["sh", "-c"]:
                if os.environ.get("FAKE_SERVICE_WRITE_DENIED") == "1":
                    sys.exit(1)
                completed = subprocess.run(
                    ["/bin/sh", *rest[2:]],
                    env=os.environ,
                )
                sys.exit(completed.returncode)

            if command == "test":
                predicate, path = rest[-2], mapped(rest[-1])
                matches = {
                    "-e": path.exists(),
                    "-L": path.is_symlink(),
                }
                sys.exit(0 if matches.get(predicate, False) else 1)

            if command == "cp":
                source, destination = mapped(rest[-2]), mapped(rest[-1])
                if (
                    os.environ.get("FAKE_FAIL_RESTORE_CP") == "1"
                    and ".backup." in str(source)
                    and str(destination).endswith(".service")
                ):
                    sys.exit(1)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination, follow_symlinks=False)
                sys.exit(0)

            if command == "install":
                if "-d" in rest:
                    mapped(rest[-1]).mkdir(parents=True, exist_ok=True)
                else:
                    source, destination = mapped(rest[-2]), mapped(rest[-1])
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
                    destination.chmod(0o644)
                    if os.environ.get("FAKE_FAIL_STAGE_INSTALL") == "1":
                        sys.exit(1)
                sys.exit(0)

            if command == "mv":
                source, destination = mapped(rest[-2]), mapped(rest[-1])
                if os.environ.get("FAKE_FAIL_MV") == "1":
                    sys.exit(1)
                destination.parent.mkdir(parents=True, exist_ok=True)
                source.replace(destination)
                sys.exit(0)

            if command == "rm":
                if (
                    os.environ.get("FAKE_FAIL_ROLLBACK_RM") == "1"
                    and rest[-1].endswith(".service")
                ):
                    sys.exit(1)
                if (
                    os.environ.get("FAKE_FAIL_STAGE_CLEANUP") == "1"
                    and ".stage." in rest[-1]
                ):
                    sys.exit(1)
                mapped(rest[-1]).unlink(missing_ok=True)
                sys.exit(0)

            if command != "systemctl":
                raise SystemExit(f"unsupported fake sudo command: {args}")

            state_path = root / "systemctl-state.json"
            if state_path.exists():
                state = json.loads(state_path.read_text())
            else:
                state = {
                    "enable_state": os.environ.get(
                        "FAKE_ENABLE_STATE",
                        "enabled"
                        if os.environ.get("FAKE_WAS_ENABLED") == "1"
                        else "disabled",
                    ),
                    "active_state": os.environ.get(
                        "FAKE_ACTIVE_STATE",
                        "active"
                        if os.environ.get("FAKE_WAS_ACTIVE") == "1"
                        else "inactive",
                    ),
                    "restart_seen": False,
                    "restart_attempts": 0,
                    "daemon_attempts": 0,
                    "enable_attempts": 0,
                }

            action = rest[0]
            exit_code = 0
            if action == "is-enabled":
                if state["enable_state"] == "error":
                    print("Failed to connect to bus", file=sys.stderr)
                    exit_code = 1
                else:
                    print(state["enable_state"])
                    exit_code = 0 if state["enable_state"] == "enabled" else 1
            elif action == "is-active":
                if "--quiet" in rest:
                    should_fail = (
                        os.environ.get("FAKE_FAIL_IS_ACTIVE") == "1"
                        and state["restart_seen"]
                    )
                    exit_code = (
                        1
                        if should_fail or state["active_state"] != "active"
                        else 0
                    )
                elif state["active_state"] == "error":
                    print("Failed to connect to bus", file=sys.stderr)
                    exit_code = 1
                else:
                    print(state["active_state"])
                    exit_code = 0 if state["active_state"] == "active" else 3
            elif action == "enable":
                state["enable_attempts"] += 1
                if (
                    os.environ.get("FAKE_FAIL_ENABLE") == "1"
                    and state["enable_attempts"] == 1
                ):
                    exit_code = 1
                else:
                    state["enable_state"] = "enabled"
            elif action == "disable":
                state["enable_state"] = "disabled"
                if "--now" in rest:
                    state["active_state"] = "inactive"
            elif action == "restart":
                state["restart_seen"] = True
                state["restart_attempts"] += 1
                if (
                    os.environ.get("FAKE_FAIL_RESTART") == "1"
                    and state["restart_attempts"] == 1
                ):
                    exit_code = 1
                else:
                    state["active_state"] = "active"
            elif action == "start":
                state["active_state"] = "active"
            elif action == "stop":
                state["active_state"] = "inactive"
            elif action == "daemon-reload":
                state["daemon_attempts"] += 1
                fail_install = (
                    os.environ.get("FAKE_FAIL_INSTALL_DAEMON") == "1"
                    and state["daemon_attempts"] == 1
                )
                fail_rollback = (
                    os.environ.get("FAKE_FAIL_ROLLBACK_DAEMON") == "1"
                    and state["daemon_attempts"] == 2
                )
                if fail_install or fail_rollback:
                    exit_code = 1
            else:
                raise SystemExit(f"unsupported fake systemctl command: {rest}")

            state_path.write_text(json.dumps(state))
            sys.exit(exit_code)
            """
        ),
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "APP_DIR": str(app_dir),
            "PYTHON_BIN": str(python_bin),
            "SERVICE_NAME": SERVICE_NAME,
            "SERVICE_USER": "dashboard-user",
            "ALPHA_DASHBOARD_HOST": "127.0.0.1",
            "ALPHA_DASHBOARD_PORT": "8796",
            "ALPHA_COMPETITION_RULE_CACHE": str(
                tmp_path / ".cache/binance-alpha-volume-alert/rules.json"
            ),
            "ALPHA_COMPETITION_DISCOVERY_CACHE": str(
                tmp_path
                / ".cache/binance-alpha-volume-alert/competition_discovery.json"
            ),
            "FAKE_SYSTEM_ROOT": str(fake_root),
            "FAKE_SUDO_LOG": str(tmp_path / "sudo.log"),
            "FAKE_CHOWN_MARKER": str(tmp_path / "chown-marker.json"),
            "SIMULATED_SECRET": "must-not-appear-in-output",
        }
    )
    return env, fake_root


def _run(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(INSTALLER)],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _sudo_calls(env: dict[str, str]) -> list[list[str]]:
    log_path = Path(env["FAKE_SUDO_LOG"])
    if not log_path.exists():
        return []
    return [json.loads(line) for line in log_path.read_text().splitlines()]


def _systemctl_state(fake_root: Path) -> dict[str, object]:
    return json.loads((fake_root / "systemctl-state.json").read_text())


def test_installer_runs_repo_module_and_keeps_existing_env_files() -> None:
    text = INSTALLER.read_text()
    assert "set -euo pipefail" in text
    assert "-m grid_optimizer.alpha_competition_dashboard" in text
    assert "EnvironmentFile=-/home/ubuntu/.config/wangge/grid_web_controller.env" in text
    assert (
        "EnvironmentFile=-/home/ubuntu/.config/binance-alpha-volume-alert.env" in text
    )
    assert "ALPHA_COMPETITION_RULE_CACHE" in text
    assert (
        'ALPHA_COMPETITION_DISCOVERY_CACHE:-/home/ubuntu/.cache/'
        'binance-alpha-volume-alert/competition_discovery.json'
    ) in text
    assert 'systemctl is-active --quiet "${SERVICE_NAME}.service"' in text
    assert "rollback" in text


def test_success_installs_unit_and_does_not_rollback(
    installer_env: tuple[dict[str, str], Path],
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env.update({"FAKE_WAS_ENABLED": "1", "FAKE_WAS_ACTIVE": "1"})

    result = _run(env)

    assert result.returncode == 0, result.stderr
    unit = unit_path.read_text()
    assert f"WorkingDirectory={env['APP_DIR']}" in unit
    assert f"Environment=PYTHONPATH={env['APP_DIR']}/src" in unit
    assert f"Environment=ALPHA_COMPETITION_RULE_CACHE={env['ALPHA_COMPETITION_RULE_CACHE']}" in unit
    assert (
        "Environment=ALPHA_COMPETITION_DISCOVERY_CACHE="
        f"{env['ALPHA_COMPETITION_DISCOVERY_CACHE']}"
    ) in unit
    assert (
        f"ExecStart={env['PYTHON_BIN']} -m grid_optimizer.alpha_competition_dashboard "
        "--host 127.0.0.1 --port 8796"
    ) in unit
    assert list(unit_path.parent.glob(f"{SERVICE_NAME}.service.backup.*"))
    calls = _sudo_calls(env)
    assert ["systemctl", "daemon-reload"] in calls
    assert ["systemctl", "enable", f"{SERVICE_NAME}.service"] in calls
    assert ["systemctl", "restart", f"{SERVICE_NAME}.service"] in calls
    assert ["systemctl", "is-active", "--quiet", f"{SERVICE_NAME}.service"] in calls
    assert not any(call[:2] == ["systemctl", "disable"] for call in calls)
    unit_system_path = f"/etc/systemd/system/{SERVICE_NAME}.service"
    unit_installs = [call for call in calls if call[0] == "install" and "-d" not in call]
    assert len(unit_installs) == 1
    stage_path = unit_installs[0][-1]
    assert stage_path != unit_system_path
    assert stage_path.startswith(f"{unit_system_path}.stage.")
    assert ["mv", stage_path, unit_system_path] in calls
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.stage.*"))
    cache_dir = Path(env["ALPHA_COMPETITION_RULE_CACHE"]).parent
    assert cache_dir.is_dir()
    assert cache_dir.stat().st_mode & 0o777 == 0o750
    secure_prepares = [call for call in calls if call[:3] == ["bash", "-s", "--"]]
    assert len(secure_prepares) == 1
    assert secure_prepares[0][-2:] == [str(cache_dir), "dashboard-user"]
    assert json.loads(Path(env["FAKE_CHOWN_MARKER"]).read_text()) == [
        "--",
        "dashboard-user:dashboard-user",
        ".",
    ]
    assert not any(cache_dir.glob(".alpha-dashboard-install-write-probe.*"))
    assert "must-not-appear-in-output" not in result.stdout + result.stderr


@pytest.mark.parametrize("failure", ["restart", "is-active"])
def test_failure_restores_old_unit_and_original_service_state(
    installer_env: tuple[dict[str, str], Path], failure: str
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env.update({"FAKE_WAS_ENABLED": "1", "FAKE_WAS_ACTIVE": "1"})
    env["FAKE_FAIL_RESTART" if failure == "restart" else "FAKE_FAIL_IS_ACTIVE"] = "1"

    result = _run(env)

    assert result.returncode != 0
    assert unit_path.read_text() == "old unit\n"
    state = _systemctl_state(fake_root)
    assert state["enable_state"] == "enabled"
    assert state["active_state"] == "active"
    calls = _sudo_calls(env)
    assert calls.count(["systemctl", "restart", f"{SERVICE_NAME}.service"]) == 2
    assert ["systemctl", "start", f"{SERVICE_NAME}.service"] not in calls
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.stage.*"))
    assert "must-not-appear-in-output" not in result.stdout + result.stderr


def test_failure_without_old_unit_removes_new_unit_and_disables_service(
    installer_env: tuple[dict[str, str], Path],
) -> None:
    env, fake_root = installer_env
    env["FAKE_FAIL_IS_ACTIVE"] = "1"

    result = _run(env)

    assert result.returncode != 0
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    assert not unit_path.exists()
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.stage.*"))
    assert _systemctl_state(fake_root)["enable_state"] == "disabled"
    assert [
        "systemctl",
        "disable",
        "--now",
        f"{SERVICE_NAME}.service",
    ] in _sudo_calls(env)


@pytest.mark.parametrize(
    "failure_var",
    [
        "FAKE_FAIL_STAGE_INSTALL",
        "FAKE_FAIL_MV",
        "FAKE_FAIL_INSTALL_DAEMON",
        "FAKE_FAIL_ENABLE",
    ],
)
def test_install_failure_matrix_restores_old_unit_without_stage_residue(
    installer_env: tuple[dict[str, str], Path], failure_var: str
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env.update(
        {
            "FAKE_WAS_ENABLED": "1",
            "FAKE_WAS_ACTIVE": "1",
            failure_var: "1",
        }
    )

    result = _run(env)

    assert result.returncode == 1
    assert unit_path.read_text() == "old unit\n"
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.stage.*"))
    state = _systemctl_state(fake_root)
    assert state["enable_state"] == "enabled"
    assert state["active_state"] == "active"


def test_existing_cache_directory_is_not_chowned_or_chmodded(
    installer_env: tuple[dict[str, str], Path],
) -> None:
    env, _ = installer_env
    cache_dir = Path(env["ALPHA_COMPETITION_RULE_CACHE"]).parent
    cache_dir.mkdir(parents=True)
    cache_dir.chmod(0o711)

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert cache_dir.stat().st_mode & 0o777 == 0o711
    secure_prepares = [
        call for call in _sudo_calls(env) if call[:3] == ["bash", "-s", "--"]
    ]
    assert len(secure_prepares) == 1
    assert not any(cache_dir.glob(".alpha-dashboard-install-write-probe.*"))


def test_cache_root_race_does_not_follow_symlink_or_touch_unit(
    installer_env: tuple[dict[str, str], Path], tmp_path: Path
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    outside = tmp_path / "outside"
    env.update(
        {
            "FAKE_RACE_CACHE_ROOT": "1",
            "FAKE_RACE_OUTSIDE": str(outside),
            "FAKE_WAS_ENABLED": "1",
            "FAKE_WAS_ACTIVE": "1",
        }
    )

    result = _run(env)

    assert result.returncode != 0
    assert not (outside / "binance-alpha-volume-alert").exists()
    assert unit_path.read_text() == "old unit\n"
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.backup.*"))


def test_unwritable_existing_cache_fails_before_unit_backup(
    installer_env: tuple[dict[str, str], Path]
) -> None:
    env, fake_root = installer_env
    cache_dir = Path(env["ALPHA_COMPETITION_RULE_CACHE"]).parent
    cache_dir.mkdir(parents=True)
    cache_dir.chmod(0o711)
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env.update(
        {
            "FAKE_SERVICE_WRITE_DENIED": "1",
            "FAKE_WAS_ENABLED": "1",
            "FAKE_WAS_ACTIVE": "1",
        }
    )

    result = _run(env)

    assert result.returncode != 0
    assert unit_path.read_text() == "old unit\n"
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.backup.*"))


def test_python_path_is_canonicalized_without_resolving_venv_executable_symlink(
    installer_env: tuple[dict[str, str], Path],
) -> None:
    env, fake_root = installer_env
    python_bin = Path(env["PYTHON_BIN"])
    target = Path(env["APP_DIR"]).parent / "python-target"
    _write_executable(target, "#!/bin/sh\nexit 0\n")
    python_bin.unlink()
    python_bin.symlink_to(target)

    result = _run(env)

    assert result.returncode == 0, result.stderr
    unit = (fake_root / "systemd" / f"{SERVICE_NAME}.service").read_text()
    assert f"ExecStart={python_bin} -m grid_optimizer.alpha_competition_dashboard" in unit
    assert f"ExecStart={target} " not in unit


@pytest.mark.parametrize(
    "cache_variable",
    ["ALPHA_COMPETITION_RULE_CACHE", "ALPHA_COMPETITION_DISCOVERY_CACHE"],
)
@pytest.mark.parametrize("case", ["root", "tmp", "dotdot"])
def test_rejects_cache_outside_dedicated_app_home(
    installer_env: tuple[dict[str, str], Path], cache_variable: str, case: str
) -> None:
    env, fake_root = installer_env
    app_home = Path(env["APP_DIR"]).parent
    paths = {
        "root": Path("/rules.json"),
        "tmp": Path("/tmp/rules.json"),
        "dotdot": app_home
        / ".cache/binance-alpha-volume-alert/../escape/rules.json",
    }
    env[cache_variable] = str(paths[case])

    result = _run(env)

    assert result.returncode != 0
    calls = _sudo_calls(env)
    assert not calls
    assert not (fake_root / "systemd" / f"{SERVICE_NAME}.service").exists()


def test_rejects_symlinked_cache_root_without_sudo_touch(
    installer_env: tuple[dict[str, str], Path], tmp_path: Path
) -> None:
    env, _ = installer_env
    cache_dir = Path(env["ALPHA_COMPETITION_RULE_CACHE"]).parent
    cache_dir.parent.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    cache_dir.symlink_to(outside, target_is_directory=True)

    result = _run(env)

    assert result.returncode != 0
    assert not _sudo_calls(env)


@pytest.mark.parametrize(
    ("target", "enable_state"),
    [("/dev/null", "masked"), ("/missing-dashboard-unit", "bad")],
)
def test_masked_or_broken_symlink_unit_is_rejected_before_changes(
    installer_env: tuple[dict[str, str], Path], target: str, enable_state: str
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.symlink_to(target)
    env.update(
        {
            "FAKE_ENABLE_STATE": enable_state,
            "FAKE_ACTIVE_STATE": "inactive",
        }
    )

    result = _run(env)

    assert result.returncode != 0
    assert "Unsupported existing unit state" in result.stderr
    assert unit_path.is_symlink()
    assert os.readlink(unit_path) == target
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.backup.*"))
    assert not list(unit_path.parent.glob(f"{SERVICE_NAME}.service.stage.*"))
    assert not Path(env["ALPHA_COMPETITION_RULE_CACHE"]).parent.exists()
    calls = _sudo_calls(env)
    assert not any(call[0] in {"cp", "install", "mv", "rm"} for call in calls)
    assert not any(
        call[:2]
        in (
            ["systemctl", "enable"],
            ["systemctl", "disable"],
            ["systemctl", "restart"],
            ["systemctl", "stop"],
        )
        for call in calls
    )


@pytest.mark.parametrize("query", ["enable", "active"])
def test_status_query_error_stops_before_any_unit_or_cache_change(
    installer_env: tuple[dict[str, str], Path], query: str
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env["FAKE_ENABLE_STATE" if query == "enable" else "FAKE_ACTIVE_STATE"] = (
        "error"
    )

    result = _run(env)

    assert result.returncode != 0
    assert unit_path.read_text() == "old unit\n"
    assert not Path(env["ALPHA_COMPETITION_RULE_CACHE"]).parent.exists()
    mutating_commands = {"cp", "install", "mv", "rm"}
    calls = _sudo_calls(env)
    assert not any(call[0] in mutating_commands for call in calls)
    assert not any(
        call[:2] in (["systemctl", "enable"], ["systemctl", "stop"])
        for call in calls
    )


def test_known_failed_service_state_is_restored_as_stopped(
    installer_env: tuple[dict[str, str], Path],
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env.update(
        {
            "FAKE_ENABLE_STATE": "disabled",
            "FAKE_ACTIVE_STATE": "failed",
            "FAKE_FAIL_RESTART": "1",
        }
    )

    result = _run(env)

    assert result.returncode == 1
    assert unit_path.read_text() == "old unit\n"
    calls = _sudo_calls(env)
    assert ["systemctl", "disable", f"{SERVICE_NAME}.service"] in calls
    assert ["systemctl", "stop", f"{SERVICE_NAME}.service"] in calls


@pytest.mark.parametrize(
    ("failure_var", "marker", "expected_old_unit"),
    [
        ("FAKE_FAIL_ROLLBACK_RM", "remove-current-unit", False),
        ("FAKE_FAIL_RESTORE_CP", "restore-unit", False),
        ("FAKE_FAIL_ROLLBACK_DAEMON", "daemon-reload", True),
    ],
)
def test_rollback_failure_stops_before_restarting_new_unit(
    installer_env: tuple[dict[str, str], Path],
    failure_var: str,
    marker: str,
    expected_old_unit: bool,
) -> None:
    env, fake_root = installer_env
    unit_path = fake_root / "systemd" / f"{SERVICE_NAME}.service"
    unit_path.write_text("old unit\n")
    env.update(
        {
            "FAKE_WAS_ENABLED": "1",
            "FAKE_WAS_ACTIVE": "1",
            "FAKE_FAIL_RESTART": "1",
            failure_var: "1",
        }
    )

    result = _run(env)

    assert result.returncode == 1
    calls = _sudo_calls(env)
    assert calls.count(["systemctl", "restart", f"{SERVICE_NAME}.service"]) == 1
    assert calls.count(["systemctl", "enable", f"{SERVICE_NAME}.service"]) == 1
    assert f"Rollback failed at {marker}" in result.stderr
    if expected_old_unit:
        assert unit_path.read_text() == "old unit\n"
    else:
        assert "Binance Alpha competition dashboard" in unit_path.read_text()


def test_stage_cleanup_failure_warns_without_failing_success(
    installer_env: tuple[dict[str, str], Path],
) -> None:
    env, fake_root = installer_env
    env["FAKE_FAIL_STAGE_CLEANUP"] = "1"

    result = _run(env)

    assert result.returncode == 0
    assert "Warning: failed to clean unit stage" in result.stderr
    assert (fake_root / "systemd" / f"{SERVICE_NAME}.service").is_file()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SERVICE_NAME", "bad/name"),
        ("SERVICE_NAME", "-looks-like-an-option"),
        ("SERVICE_NAME", "already-has.service"),
        ("ALPHA_DASHBOARD_HOST", "127.0.0.1\\nExecStart=/bin/false"),
        ("ALPHA_DASHBOARD_PORT", "70000"),
        ("ALPHA_COMPETITION_RULE_CACHE", "/tmp/cache path/rules.json"),
        (
            "ALPHA_COMPETITION_DISCOVERY_CACHE",
            "/tmp/cache path/competition_discovery.json",
        ),
    ],
)
def test_rejects_unsafe_unit_values(
    installer_env: tuple[dict[str, str], Path], name: str, value: str
) -> None:
    env, fake_root = installer_env
    env[name] = value

    result = _run(env)

    assert result.returncode != 0
    assert not (fake_root / "systemd" / f"{SERVICE_NAME}.service").exists()
