#!/usr/bin/env python3
"""Roll an expired runner control window to the current Beijing trade day."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo


BEIJING = ZoneInfo("Asia/Shanghai")


def current_trade_window(now: datetime, reset_hour: int) -> tuple[datetime, datetime]:
    local_now = now.astimezone(BEIJING)
    start = local_now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)
    if local_now < start:
        start -= timedelta(days=1)
    return start, start + timedelta(days=1)


def roll_control_window(
    control: dict[str, object], *, now: datetime, reset_hour: int
) -> dict[str, object]:
    start, end = current_trade_window(now, reset_hour)
    configured_end = str(control.get("run_end_time") or "")
    if configured_end:
        try:
            existing_end = datetime.fromisoformat(configured_end)
        except ValueError:
            existing_end = None
        if existing_end is not None and existing_end.astimezone(BEIJING) > now.astimezone(BEIJING):
            return control

    updated = dict(control)
    updated["run_start_time"] = start.isoformat()
    updated["run_end_time"] = end.isoformat()
    updated["runtime_guard_stats_start_time"] = start.isoformat()
    updated["competition_window_auto_rolled_at"] = now.astimezone(BEIJING).isoformat()
    return updated


def write_json_atomically(path: Path, payload: dict[str, object]) -> None:
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    temporary_path.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-path", type=Path, required=True)
    parser.add_argument("--reset-hour", type=int, default=8)
    args = parser.parse_args()
    if not 0 <= args.reset_hour <= 23:
        raise SystemExit("--reset-hour must be between 0 and 23")

    control_path = args.control_path
    control = json.loads(control_path.read_text(encoding="utf-8"))
    if not isinstance(control, dict):
        raise SystemExit("runner control must be a JSON object")
    now = datetime.now(BEIJING)
    updated = roll_control_window(control, now=now, reset_hour=args.reset_hour)
    changed = updated != control
    if changed:
        write_json_atomically(control_path, updated)
    print(
        json.dumps(
            {
                "changed": changed,
                "run_start_time": updated.get("run_start_time"),
                "run_end_time": updated.get("run_end_time"),
                "runtime_guard_stats_start_time": updated.get("runtime_guard_stats_start_time"),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
