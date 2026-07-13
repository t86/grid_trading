#!/usr/bin/env python3
"""Enable ARX single-leg profitable frozen inventory release safely."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


UPDATES = {
    "best_quote_maker_volume_reduce_freeze_loss_ratio": 0.01,
    "best_quote_maker_volume_frozen_pair_release_enabled": False,
    "best_quote_maker_volume_frozen_single_leg_take_profit_enabled": True,
    "best_quote_maker_volume_frozen_pair_release_min_profit_ratio": 0.002,
    "best_quote_maker_volume_frozen_pair_release_max_notional": 20.0,
}

REQUIRED_SAFETY = {
    "best_quote_maker_volume_frozen_pair_release_allow_loss": False,
    "best_quote_maker_volume_net_loss_reduce_enabled": False,
    "hard_loss_forced_reduce_enabled": False,
}

EXPECTED_LIMITS = {
    "best_quote_maker_volume_reduce_freeze_band_budget_base_notional": 100.0,
    "best_quote_maker_volume_frozen_total_cap_notional": 800.0,
}


def configure(path: Path) -> dict[str, object]:
    original = json.loads(path.read_text())
    if str(original.get("symbol") or "").upper() != "ARXUSDT":
        raise ValueError(f"refusing non-ARX control file: {path}")

    for key, expected in REQUIRED_SAFETY.items():
        if bool(original.get(key, False)) is not expected:
            raise ValueError(f"unsafe {key}={original.get(key)!r}; expected {expected!r}")
    for key, expected in EXPECTED_LIMITS.items():
        if float(original.get(key, 0.0)) != expected:
            raise ValueError(f"unexpected {key}={original.get(key)!r}; expected {expected!r}")

    updated = dict(original)
    updated.update(UPDATES)
    changed = {key: {"old": original.get(key), "new": value} for key, value in UPDATES.items() if original.get(key) != value}
    if not changed:
        return {"path": str(path), "changed": {}, "backup": None}

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(f"{path.name}.bak_single_leg_freeze_{stamp}")
    shutil.copy2(path, backup)
    path.write_text(json.dumps(updated, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return {"path": str(path), "changed": changed, "backup": str(backup)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("control_path", type=Path)
    args = parser.parse_args()
    print(json.dumps(configure(args.control_path), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
