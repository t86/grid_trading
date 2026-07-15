from __future__ import annotations

import stat
from pathlib import Path
from tempfile import TemporaryDirectory

from grid_optimizer.recovery_control_ownership import write_control_json_atomically


def test_atomic_control_write_is_readable_by_the_runner_service_user() -> None:
    with TemporaryDirectory() as temp_dir:
        control_path = Path(temp_dir) / "arxusdt_loop_runner_control.json"
        write_control_json_atomically(control_path, {"symbol": "ARXUSDT"})

        assert control_path.read_text(encoding="utf-8")
        assert stat.S_IMODE(control_path.stat().st_mode) == 0o644
