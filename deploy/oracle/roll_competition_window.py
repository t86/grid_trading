#!/usr/bin/env python3
"""Roll an expired runner control window to the current Beijing trade day."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from zoneinfo import ZoneInfo


BEIJING = ZoneInfo("Asia/Shanghai")

# These fields define the durable competition baseline.  The runtime profile is
# their sole value source; the saved runner control may only contain temporary
# recovery overrides between daily resets.
PROFILE_BASELINE_KEYS = (
    "step_price",
    "maker_order_notional",
    "best_quote_maker_volume_cycle_budget_notional",
    "best_quote_maker_volume_min_cycle_budget_notional",
    "best_quote_maker_volume_max_long_notional",
    "best_quote_maker_volume_max_short_notional",
    "best_quote_maker_volume_inventory_soft_ratio",
    "pause_buy_position_notional",
    "pause_short_position_notional",
    "threshold_position_notional",
    "max_position_notional",
    "max_short_position_notional",
    "max_total_notional",
    "max_actual_net_notional",
    "execution_place_budget_per_cycle",
    "max_new_orders",
    "best_quote_maker_volume_allow_loss_reduce_only",
    "best_quote_maker_volume_net_loss_reduce_enabled",
    "hard_loss_forced_reduce_enabled",
    "best_quote_maker_volume_reduce_freeze_enabled",
    "best_quote_maker_volume_reduce_freeze_loss_ratio",
    "best_quote_maker_volume_reduce_freeze_min_notional",
    "best_quote_maker_volume_reduce_freeze_confirm_cycles",
    "best_quote_maker_volume_reduce_freeze_hard_loss_ratio",
    "best_quote_maker_volume_reduce_freeze_hard_min_notional",
    "best_quote_maker_volume_reduce_freeze_hard_confirm_cycles",
    "best_quote_maker_volume_reduce_freeze_stress_loss_ratio",
    "best_quote_maker_volume_reduce_freeze_stress_1m_abs_return_ratio",
    "best_quote_maker_volume_reduce_freeze_stress_1m_amplitude_ratio",
    "best_quote_maker_volume_reduce_freeze_profitable_pair_gate_enabled",
    "best_quote_maker_volume_reduce_freeze_band_budget_enabled",
    "best_quote_maker_volume_reduce_freeze_band_budget_price_ratio",
    "best_quote_maker_volume_reduce_freeze_band_budget_base_notional",
    "best_quote_maker_volume_frozen_long_cap_notional",
    "best_quote_maker_volume_frozen_short_cap_notional",
    "best_quote_maker_volume_frozen_total_cap_notional",
    "best_quote_maker_volume_frozen_pair_release_enabled",
    "best_quote_maker_volume_frozen_pair_release_allow_loss",
    "best_quote_maker_volume_reduce_freeze_quality_gate_enabled",
    "best_quote_maker_volume_reduce_freeze_quality_max_loss_ratio",
    "best_quote_maker_volume_reduce_freeze_quality_release_profit_ratio",
    "best_quote_maker_volume_reduce_freeze_quality_max_atr_multiple",
    "best_quote_maker_volume_reduce_freeze_quality_atr_floor_ratio",
    "best_quote_maker_volume_reduce_freeze_quality_easy_bucket_notional",
    "best_quote_maker_volume_reduce_freeze_quality_medium_bucket_notional",
    "best_quote_maker_volume_reduce_freeze_quality_hard_bucket_notional",
    "best_quote_maker_volume_same_side_entry_price_guard_enabled",
    "best_quote_maker_volume_same_side_entry_price_guard_min_notional",
    "best_quote_maker_volume_same_side_entry_price_guard_gap_ticks",
)

RECOVERY_OVERLAY_STATE_KEYS = (
    "guard_original_controls",
    "guard_recovery_controls",
    "recovery_owned",
    "recovery_started_at",
    "net_guard_recovery_baseline_notional",
    "net_guard_recovery_direction",
    "post_restore_cooldown_until",
)

REQUIRED_CONTROL_KEYS = (
    "symbol",
    "strategy_profile",
    "step_price",
    "max_actual_net_notional",
)

TARGET_NOTIONAL_KEYS = (
    "best_quote_maker_volume_target_remaining_notional",
    "max_cumulative_notional",
)


def current_trade_window(now: datetime, reset_hour: int) -> tuple[datetime, datetime]:
    local_now = now.astimezone(BEIJING)
    start = local_now.replace(hour=reset_hour, minute=0, second=0, microsecond=0)
    if local_now < start:
        start -= timedelta(days=1)
    return start, start + timedelta(days=1)


def roll_control_window(
    control: dict[str, object],
    *,
    now: datetime,
    reset_hour: int,
    runtime_profile: dict[str, object] | None = None,
    force_profile_rebase: bool = False,
) -> dict[str, object]:
    start, end = current_trade_window(now, reset_hour)
    configured_end = str(control.get("run_end_time") or "")
    if configured_end:
        try:
            existing_end = datetime.fromisoformat(configured_end)
        except ValueError:
            existing_end = None
        if existing_end is not None and existing_end.astimezone(BEIJING) > now.astimezone(BEIJING):
            if not force_profile_rebase:
                return control
            updated = dict(control)
            rolled = False
        else:
            updated = dict(control)
            rolled = True
    else:
        updated = dict(control)
        rolled = True

    if rolled:
        updated["run_start_time"] = start.isoformat()
        updated["run_end_time"] = end.isoformat()
        updated["runtime_guard_stats_start_time"] = start.isoformat()
        updated["competition_window_auto_rolled_at"] = now.astimezone(BEIJING).isoformat()
    if runtime_profile and (rolled or force_profile_rebase):
        for key in PROFILE_BASELINE_KEYS:
            if key in runtime_profile:
                updated[key] = runtime_profile[key]
        updated["competition_control_profile_rebased_at"] = now.astimezone(BEIJING).isoformat()
    return updated


def clear_recovery_overlay(state: dict[str, object], *, symbol: str) -> bool:
    symbols = state.get("symbols")
    if not isinstance(symbols, dict):
        return False
    item = symbols.get(symbol)
    if not isinstance(item, dict):
        return False
    changed = False
    for key in RECOVERY_OVERLAY_STATE_KEYS:
        if key in item:
            item.pop(key, None)
            changed = True
    return changed


def apply_target_notional(control: dict[str, object], *, target_notional: float | None) -> bool:
    """Keep the runner's pace target and hard daily ceiling in one update."""
    if target_notional is None:
        return False
    target = float(target_notional)
    if target <= 0:
        raise ValueError("target_notional must be positive")
    changed = False
    for key in TARGET_NOTIONAL_KEYS:
        if control.get(key) != target:
            control[key] = target
            changed = True
    return changed


def reset_runtime_guard_baseline(
    control: dict[str, object],
    state: dict[str, object] | None,
    *,
    now: datetime,
) -> tuple[bool, bool]:
    """Start a fresh guard accounting interval without touching audit or inventory."""
    control_changed = control.get("runtime_guard_stats_start_time") != now.isoformat()
    control["runtime_guard_stats_start_time"] = now.isoformat()
    state_changed = False
    if isinstance(state, dict) and "runtime_guard_loss_recovery" in state:
        state.pop("runtime_guard_loss_recovery", None)
        state_changed = True
    return control_changed, state_changed


def load_usable_control(control_path: Path) -> tuple[dict[str, object], str | None]:
    """Use the newest complete backup when a concurrent writer damaged control."""
    candidates = [control_path] + sorted(
        control_path.parent.glob(f"{control_path.name}.bak_*"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if any(key not in payload for key in REQUIRED_CONTROL_KEYS):
            continue
        recovered_from = None if candidate == control_path else str(candidate)
        return payload, recovered_from
    raise SystemExit("no complete runner control or valid control backup was found")


def write_json_atomically(path: Path, payload: dict[str, object]) -> None:
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    # Deployment jobs may run as root while the runner runs unprivileged.
    # Control documents carry no credentials, so keep them runner-readable.
    os.chmod(temporary_path, 0o644)
    temporary_path.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-path", type=Path, required=True)
    parser.add_argument("--runtime-profile", type=Path)
    parser.add_argument("--guard-state-path", type=Path)
    parser.add_argument("--symbol", default="ARXUSDT")
    parser.add_argument("--force-profile-rebase", action="store_true")
    parser.add_argument("--target-notional", type=float)
    parser.add_argument("--loop-state-path", type=Path)
    parser.add_argument("--reset-runtime-guard-baseline", action="store_true")
    parser.add_argument("--reset-hour", type=int, default=8)
    args = parser.parse_args()
    if not 0 <= args.reset_hour <= 23:
        raise SystemExit("--reset-hour must be between 0 and 23")
    if args.target_notional is not None and args.target_notional <= 0:
        raise SystemExit("--target-notional must be positive")

    control_path = args.control_path
    control, recovered_from = load_usable_control(control_path)
    runtime_profile: dict[str, object] | None = None
    if args.runtime_profile is not None:
        runtime_profile = json.loads(args.runtime_profile.read_text(encoding="utf-8"))
        if not isinstance(runtime_profile, dict):
            raise SystemExit("runtime profile must be a JSON object")
    now = datetime.now(BEIJING)
    updated = roll_control_window(
        control,
        now=now,
        reset_hour=args.reset_hour,
        runtime_profile=runtime_profile,
        force_profile_rebase=args.force_profile_rebase,
    )
    target_changed = apply_target_notional(updated, target_notional=args.target_notional)
    loop_state: dict[str, object] | None = None
    if args.loop_state_path is not None and args.loop_state_path.exists():
        raw_state = json.loads(args.loop_state_path.read_text(encoding="utf-8"))
        if isinstance(raw_state, dict):
            loop_state = raw_state
    guard_baseline_changed = False
    loss_recovery_cleared = False
    if args.reset_runtime_guard_baseline:
        guard_baseline_changed, loss_recovery_cleared = reset_runtime_guard_baseline(
            updated,
            loop_state,
            now=now,
        )
    changed = (
        updated != control
        or recovered_from is not None
        or target_changed
        or guard_baseline_changed
    )
    if changed:
        write_json_atomically(control_path, updated)
    if loss_recovery_cleared and args.loop_state_path is not None and loop_state is not None:
        write_json_atomically(args.loop_state_path, loop_state)
    state_changed = False
    if changed and args.guard_state_path is not None and args.guard_state_path.exists():
        state = json.loads(args.guard_state_path.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            state_changed = clear_recovery_overlay(state, symbol=str(args.symbol).upper())
            if state_changed:
                write_json_atomically(args.guard_state_path, state)
    print(
        json.dumps(
            {
                "changed": changed,
                "run_start_time": updated.get("run_start_time"),
                "run_end_time": updated.get("run_end_time"),
                "runtime_guard_stats_start_time": updated.get("runtime_guard_stats_start_time"),
                "profile_rebased": bool(runtime_profile) and changed,
                "recovery_overlay_cleared": state_changed,
                "target_notional": args.target_notional,
                "target_changed": target_changed,
                "runtime_guard_baseline_reset": guard_baseline_changed,
                "runtime_guard_loss_recovery_cleared": loss_recovery_cleared,
                "control_recovered_from": recovered_from,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
