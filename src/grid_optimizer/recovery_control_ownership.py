"""Ownership boundary for recovery-coordinated futures runners.

Any control document containing the coordinator state envelope is explicitly
registered, regardless of symbol or envelope validity.  Legacy monitors may
observe it, but must not patch control, restart the runner, or execute repair
code.  Presence is the ownership fence; envelope validation belongs solely to
the coordinator/store path and must never fall back to a legacy actuator.
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterator


RECOVERY_CONTROL_OWNER = "bq_volume_recovery_guard"
DEFAULT_RECOVERY_MANAGED_SYMBOLS = frozenset({"ARXUSDT", "OUSDT"})
_RECOVERY_STATE_KEY = "_futures_recovery_state"
_RECOVERY_STATE_MIRROR_KEY = "_futures_recovery_state_mirror"


def is_recovery_managed(symbol: str, control: dict[str, Any] | None = None) -> bool:
    """Whether only the recovery guard may actuate this symbol."""
    if str(symbol).upper().strip() in DEFAULT_RECOVERY_MANAGED_SYMBOLS:
        return True
    if isinstance(control, dict) and (
        _RECOVERY_STATE_KEY in control or _RECOVERY_STATE_MIRROR_KEY in control
    ):
        return True
    return bool(control) and str(control.get("recovery_control_owner") or "") == RECOVERY_CONTROL_OWNER


def mark_recovery_owned(control: dict[str, Any]) -> None:
    control["recovery_control_owner"] = RECOVERY_CONTROL_OWNER


def write_control_json_atomically(control_path: Path, payload: dict[str, Any]) -> None:
    """Replace a control document without exposing a partial shared ``.tmp``."""
    control_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=control_path.parent,
        prefix=f".{control_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    # The guard can run as root while the symbol runner runs as an unprivileged
    # service user.  Control is not secret material (credentials live in the
    # service environment), so the replacement must remain readable by that
    # runner after os.replace.
    os.chmod(temporary_path, 0o644)
    os.replace(temporary_path, control_path)


@contextmanager
def exclusive_control_lock(control_path: Path, *, timeout_seconds: float = 15.0) -> Iterator[None]:
    """Take the per-symbol cross-process actuator lock with a bounded wait."""
    lock_path = control_path.with_suffix(control_path.suffix + ".actuator.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(float(timeout_seconds), 0.0)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for control actuator lock: {control_path}")
                time.sleep(0.05)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
