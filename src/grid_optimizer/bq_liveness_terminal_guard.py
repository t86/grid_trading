from __future__ import annotations

import argparse
import json
import runpy
import sys
from datetime import datetime, timezone
from pathlib import Path


def target_done_marker(*, workdir: Path, symbol: str, now: datetime) -> Path:
    day = now.astimezone(timezone.utc).strftime("%Y%m%d")
    return workdir / "output" / f"{symbol.lower()}_target_gate_done_{day}.flag"


def invalid_state_reason(*, workdir: Path, symbol: str) -> dict[str, str] | None:
    state_path = workdir / "output" / f"{symbol.lower()}_loop_state.json"
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return {"state_path": str(state_path), "error": str(exc)}
    if not isinstance(payload, dict):
        return {"state_path": str(state_path), "error": "state payload is not an object"}
    return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--workdir", required=True)
    args, _ = parser.parse_known_args(argv)
    workdir = Path(args.workdir).resolve()
    symbol = str(args.symbol).upper().strip()
    marker = target_done_marker(
        workdir=workdir,
        symbol=symbol,
        now=datetime.now(timezone.utc),
    )
    if marker.exists():
        print(json.dumps({"action": "skip_target_gate_done_terminal", "marker": str(marker)}))
        return 0

    invalid_state = invalid_state_reason(workdir=workdir, symbol=symbol)
    if invalid_state is not None:
        print(json.dumps({"action": "skip_invalid_state_safety_gate", **invalid_state}))
        return 0

    legacy = workdir / "output" / "ops" / "bq_liveness_watchdog.py"
    if not legacy.exists():
        raise FileNotFoundError(f"Legacy liveness watchdog not found: {legacy}")
    original_argv = sys.argv
    try:
        sys.argv = [str(legacy), *(argv if argv is not None else original_argv[1:])]
        runpy.run_path(str(legacy), run_name="__main__")
    finally:
        sys.argv = original_argv
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
