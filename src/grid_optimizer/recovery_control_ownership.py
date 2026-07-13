"""Ownership boundary for the recovery-managed futures runners.

The recovery guard is the sole control writer for the initial managed rollout
(ARXUSDT/OUSDT).  Other monitors may still observe these symbols, but must not
patch their control JSON, restart their runner, or execute legacy repair code.
An explicit owner marker lets the same contract be extended without expanding
the default rollout set.
"""
from __future__ import annotations

import fcntl
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


RECOVERY_CONTROL_OWNER = "bq_volume_recovery_guard"
DEFAULT_RECOVERY_MANAGED_SYMBOLS = frozenset({"ARXUSDT", "OUSDT"})


def is_recovery_managed(symbol: str, control: dict[str, Any] | None = None) -> bool:
    """Whether only the recovery guard may actuate this symbol."""
    if str(symbol).upper().strip() in DEFAULT_RECOVERY_MANAGED_SYMBOLS:
        return True
    return bool(control) and str(control.get("recovery_control_owner") or "") == RECOVERY_CONTROL_OWNER


def mark_recovery_owned(control: dict[str, Any]) -> None:
    control["recovery_control_owner"] = RECOVERY_CONTROL_OWNER


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
