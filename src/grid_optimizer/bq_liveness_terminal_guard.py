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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--workdir", required=True)
    args, _ = parser.parse_known_args(argv)
    workdir = Path(args.workdir).resolve()
    marker = target_done_marker(
        workdir=workdir,
        symbol=str(args.symbol).upper().strip(),
        now=datetime.now(timezone.utc),
    )
    if marker.exists():
        print(json.dumps({"action": "skip_target_gate_done_terminal", "marker": str(marker)}))
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
