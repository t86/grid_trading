from __future__ import annotations

import argparse
import atexit
import os
import sys
from pathlib import Path

from .monitor import RUNNER_PID_PATH, runner_pid_path_for_symbol
from .web import (
    _build_runner_command,
    _build_spot_runner_command,
    _load_runner_control_config,
    _load_spot_runner_control_config,
    _runner_control_path,
    _spot_runner_control_path,
)


DEFAULT_RUNNER_SYMBOL = "SOONUSDT"
RUNTIME_PATH_FLAGS = {
    "--plan-json",
    "--state-path",
    "--submit-report-json",
    "--summary-jsonl",
}


def _write_pid(path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(os.getpid()), encoding="utf-8")


def _cleanup_pid(path) -> None:
    try:
        if path.exists():
            current = path.read_text(encoding="utf-8").strip()
            if current == str(os.getpid()):
                path.unlink()
    except OSError:
        pass


def _anchor_relative_runtime_paths(command: list[str], runner_work_dir: str) -> list[str]:
    if not runner_work_dir:
        return command
    anchored = list(command)
    base = Path(runner_work_dir)
    for index, token in enumerate(anchored[:-1]):
        if token not in RUNTIME_PATH_FLAGS:
            continue
        path = Path(anchored[index + 1])
        if path.is_absolute():
            continue
        anchored[index + 1] = str(base / path)
    return anchored


def _should_use_spot_runner(symbol: str) -> bool:
    normalized = str(symbol or "").upper().strip()
    if not normalized:
        return False
    return _spot_runner_control_path(normalized).exists() and not _runner_control_path(normalized).exists()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="")
    args = parser.parse_args()

    original_cwd = os.getcwd()
    runner_work_dir = str(os.environ.get("GRID_RUNNER_WORK_DIR") or "").strip()
    if runner_work_dir:
        os.chdir(runner_work_dir)

    symbol = str(args.symbol or os.environ.get("GRID_RUNNER_SYMBOL") or DEFAULT_RUNNER_SYMBOL).upper().strip()
    if symbol and not os.environ.get("GRID_RUNNER_SERVICE_TEMPLATE"):
        os.environ["GRID_RUNNER_SERVICE_TEMPLATE"] = "grid-loop@{symbol}.service"
    pid_path = runner_pid_path_for_symbol(symbol) if symbol else RUNNER_PID_PATH
    pid_path = Path(pid_path).resolve()
    _write_pid(pid_path)
    atexit.register(_cleanup_pid, pid_path)
    if _should_use_spot_runner(symbol):
        config = _load_spot_runner_control_config(symbol)
        command_builder = _build_spot_runner_command
    else:
        config = _load_runner_control_config(symbol)
        command_builder = _build_runner_command
    command = command_builder(config)
    command = _anchor_relative_runtime_paths(command, runner_work_dir)
    if runner_work_dir:
        os.chdir(original_cwd)
    exec_env = os.environ.copy()
    exec_env.setdefault("GRID_AUTO_RESET_ON_CONFIG_CHANGE", "1")
    os.execvpe(command[0], command, exec_env)


if __name__ == "__main__":
    main()
