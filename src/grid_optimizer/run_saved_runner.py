from __future__ import annotations

import argparse
import atexit
import os
import sys

from .monitor import RUNNER_PID_PATH, runner_pid_path_for_symbol
from .web import _build_runner_command, _load_runner_control_config


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="")
    args = parser.parse_args()

    symbol = str(args.symbol or "").upper().strip()
    pid_path = runner_pid_path_for_symbol(symbol) if symbol else RUNNER_PID_PATH
    _write_pid(pid_path)
    atexit.register(_cleanup_pid, pid_path)
    command = _build_runner_command(_load_runner_control_config(symbol or None))
    os.execvpe(command[0], command, os.environ.copy())


if __name__ == "__main__":
    main()
