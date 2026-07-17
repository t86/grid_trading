from __future__ import annotations

import stat
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from grid_optimizer.recovery_control_ownership import (
    exclusive_control_lock,
    write_control_json_atomically,
)


def test_atomic_control_write_is_readable_by_the_runner_service_user() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        write_control_json_atomically(control_path, {"symbol": "ARXUSDT"})

        assert control_path.read_text(encoding="utf-8")
        assert stat.S_IMODE(control_path.stat().st_mode) == 0o644


def test_control_lock_accepts_root_created_read_only_lockfile() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        lock_path = control_path.with_suffix(
            control_path.suffix + ".actuator.lock"
        )
        lock_path.write_text("", encoding="utf-8")
        lock_path.chmod(0o644)
        original_open = Path.open

        def reject_lockfile_write_open(
            target: Path,
            mode: str = "r",
            *args: object,
            **kwargs: object,
        ):
            if (
                target.resolve(strict=False) == lock_path.resolve(strict=False)
                and any(flag in mode for flag in ("a", "w", "+"))
            ):
                raise PermissionError("runner cannot write root-owned lockfile")
            return original_open(target, mode, *args, **kwargs)

        with patch.object(Path, "open", new=reject_lockfile_write_open):
            with exclusive_control_lock(control_path, timeout_seconds=0.05):
                pass

        assert stat.S_IMODE(lock_path.stat().st_mode) == 0o644
