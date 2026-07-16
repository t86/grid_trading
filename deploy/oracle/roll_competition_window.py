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

from grid_optimizer.futures_run_lifecycle import (
    RUN_CONTRACT_OWNER_KEY,
    bind_run_contract_owner,
    validate_run_contract,
)
from grid_optimizer.futures_recovery_coordinator import (
    ActionId,
    ActionMode,
    EffectStage,
    FlowBlockerAssessment,
    FuturesRecoveryDecisionEngine,
    RecoveryPhase,
    RecoveryState,
    SymbolSnapshot,
)
from grid_optimizer.futures_recovery_store import (
    RECOVERY_STATE_MIRROR_KEY,
    RECOVERY_STATE_KEY,
    RECOVERY_STATE_SCHEMA_VERSION,
    JsonRecoveryStore,
    RecoveryStateCorruptError,
    decode_recovery_control_state,
    decode_recovery_desired_profile_fields,
    recovery_coordinator_registered,
)
from grid_optimizer.recovery_control_ownership import exclusive_control_lock


BEIJING = ZoneInfo("Asia/Shanghai")
REGISTERED_RECOVERY_DEFERRED_EXIT_CODE = 3

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
    "terminal_drain_exit_policy",
    "terminal_drain_absolute_loss_budget",
    "terminal_drain_max_wait_seconds",
    "terminal_drain_stop_preserve_reason",
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

REGISTERED_RECOVERY_LIFECYCLE_KEYS = frozenset(
    {
        "run_start_time",
        "runtime_guard_stats_start_time",
        "run_end_time",
        *TARGET_NOTIONAL_KEYS,
        RUN_CONTRACT_OWNER_KEY,
        "terminal_drain_max_order_notional",
        "competition_window_auto_rolled_at",
        "competition_control_profile_rebased_at",
    }
)


def registered_recovery_roll_status(
    control: dict[str, object],
    *,
    symbol: str,
) -> dict[str, object] | None:
    """Return why a coordinator-owned control cannot safely roll lifecycle."""

    if not recovery_coordinator_registered(control):
        return None
    normalized_symbol = str(symbol or control.get("symbol") or "").upper().strip()
    try:
        state = decode_recovery_control_state(
            control,
            expected_symbol=normalized_symbol,
        )
    except (ValueError, RecoveryStateCorruptError) as exc:
        return {
            "changed": False,
            "status": "deferred",
            "reason": "registered_recovery_state_invalid",
            "symbol": normalized_symbol,
            "phase": None,
            "stable_rebase_eligible": False,
            "atomic_rebase_required": True,
            "error": str(exc),
        }

    stable_rebase_eligible = bool(
        state.phase is RecoveryPhase.STABLE
        and state.action_lease is None
        and state.safety_lease is None
        and state.cleanup_obligation is None
        and state.pending_effect_stage is EffectStage.NONE
        and state.pending_effect_epoch is None
    )
    if not stable_rebase_eligible:
        return {
            "changed": False,
            "status": "deferred",
            "reason": "registered_recovery_state_not_safe_to_rebase",
            "symbol": state.symbol,
            "phase": state.phase.value,
            "stable_rebase_eligible": False,
            "atomic_rebase_required": True,
            "recovery_generation": state.generation,
            "recovery_document_revision": state.document_revision,
        }
    managed_lifecycle_fields = sorted(
        set(state.desired_profile.fields).intersection(
            REGISTERED_RECOVERY_LIFECYCLE_KEYS
        )
    )
    if managed_lifecycle_fields:
        return {
            "changed": False,
            "status": "deferred",
            "reason": "registered_recovery_manages_lifecycle_fields",
            "symbol": state.symbol,
            "phase": state.phase.value,
            "stable_rebase_eligible": True,
            "atomic_rebase_required": True,
            "managed_lifecycle_fields": managed_lifecycle_fields,
            "recovery_generation": state.generation,
            "recovery_document_revision": state.document_revision,
        }
    return None


def plan_registered_roll_restart(
    state: RecoveryState,
    *,
    now: datetime,
    round_id: str,
) -> RecoveryState:
    """Purely stage the coordinator-owned restart for one new run contract."""

    plan = FuturesRecoveryDecisionEngine().plan_round(
        snapshot=SymbolSnapshot(
            symbol=state.symbol,
            captured_at=now,
            assessment=FlowBlockerAssessment(
                baseline_rebase_requested=True,
            ),
        ),
        state=state,
        now=now,
        round_id=round_id,
    )
    if not (
        plan.action_id is ActionId.BASELINE_REBASE
        and plan.mode is ActionMode.ENTER
        and plan.effect_stage is EffectStage.RUNNER_RESTART
        and plan.next_state.pending_effect_stage is EffectStage.RUNNER_RESTART
        and plan.next_state.document_revision == state.document_revision + 1
        and plan.next_state.generation == state.generation + 1
    ):
        raise RuntimeError("registered roll did not stage one baseline rebase")
    return plan.next_state


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


def validate_rolled_control_contract(control: dict[str, object]) -> None:
    """Reject a new competition window that has no bounded exit contract."""

    validate_run_contract(
        run_start_time=control.get("run_start_time"),
        runtime_guard_stats_start_time=control.get("runtime_guard_stats_start_time"),
        run_end_time=control.get("run_end_time"),
        target_value=control.get("max_cumulative_notional"),
        exit_policy=control.get("terminal_drain_exit_policy"),
        loss_budget=control.get("terminal_drain_absolute_loss_budget"),
        max_wait_seconds=control.get("terminal_drain_max_wait_seconds"),
        preserve_reason=control.get("terminal_drain_stop_preserve_reason"),
    )


def bind_rolled_run_contract_owner(
    control: dict[str, object],
    *,
    now: datetime,
    handoff_reason: str | None,
) -> tuple[dict[str, object], bool]:
    """Freeze a rolled bounded run, requiring an explicit replacement reason."""

    validate_rolled_control_contract(control)
    return bind_run_contract_owner(
        control,
        activated_at=now,
        handoff_reason=handoff_reason,
    )


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
    parser.add_argument(
        "--run-contract-handoff-reason",
        help="required when replacing an already-owned bounded run contract",
    )
    parser.add_argument("--reset-hour", type=int, default=8)
    args = parser.parse_args()
    if not 0 <= args.reset_hour <= 23:
        raise SystemExit("--reset-hour must be between 0 and 23")
    if args.target_notional is not None and args.target_notional <= 0:
        raise SystemExit("--target-notional must be positive")

    control_path = args.control_path
    now = datetime.now(BEIJING)
    registered_recovery_state = None
    registered_recovery_next_state = None
    managed_profile_changes_deferred: list[str] = []
    with exclusive_control_lock(control_path):
        try:
            raw_current = json.loads(control_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            raw_current = None
        raw_registered = bool(
            isinstance(raw_current, dict)
            and recovery_coordinator_registered(raw_current)
        )
        if isinstance(raw_current, dict):
            deferred = registered_recovery_roll_status(
                raw_current,
                symbol=str(args.symbol),
            )
            if deferred is not None:
                print(json.dumps(deferred, ensure_ascii=False))
                raise SystemExit(REGISTERED_RECOVERY_DEFERRED_EXIT_CODE)

        if raw_registered:
            control = raw_current
            recovered_from = None
        else:
            control, recovered_from = load_usable_control(control_path)
            deferred = registered_recovery_roll_status(
                control,
                symbol=str(args.symbol),
            )
            if deferred is not None:
                print(json.dumps(deferred, ensure_ascii=False))
                raise SystemExit(REGISTERED_RECOVERY_DEFERRED_EXIT_CODE)
        if recovery_coordinator_registered(control):
            registered_recovery_state = decode_recovery_control_state(
                control,
                expected_symbol=str(args.symbol),
            )
        managed_desired_fields = (
            {
                key: control[key]
                for key in registered_recovery_state.desired_profile.fields
            }
            if registered_recovery_state is not None
            else {}
        )

        runtime_profile: dict[str, object] | None = None
        if args.runtime_profile is not None:
            runtime_profile = json.loads(
                args.runtime_profile.read_text(encoding="utf-8")
            )
            if not isinstance(runtime_profile, dict):
                raise SystemExit("runtime profile must be a JSON object")
        updated = roll_control_window(
            control,
            now=now,
            reset_hour=args.reset_hour,
            runtime_profile=runtime_profile,
            force_profile_rebase=args.force_profile_rebase,
        )
        target_changed = apply_target_notional(
            updated,
            target_notional=args.target_notional,
        )
        # A registered STABLE roll owns only the run lifecycle.  Runtime-profile
        # values may be observed, but the coordinator's desired strategy fields
        # remain value-for-value authoritative and its envelope is never rebuilt.
        for key, desired_value in managed_desired_fields.items():
            if key not in updated or updated[key] != desired_value:
                managed_profile_changes_deferred.append(key)
                updated[key] = desired_value
        managed_profile_changes_deferred.sort()
        try:
            validate_rolled_control_contract(updated)
        except ValueError as exc:
            raise SystemExit(f"invalid rolled run contract: {exc}") from exc
        loop_state: dict[str, object] | None = None
        if (
            registered_recovery_state is None
            and args.loop_state_path is not None
            and args.loop_state_path.exists()
        ):
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
        try:
            updated, run_contract_owner_changed = bind_rolled_run_contract_owner(
                updated,
                now=now,
                handoff_reason=args.run_contract_handoff_reason,
            )
        except ValueError as exc:
            raise SystemExit(f"invalid rolled run contract owner: {exc}") from exc
        if registered_recovery_state is not None and run_contract_owner_changed:
            owner = updated.get(RUN_CONTRACT_OWNER_KEY)
            if not isinstance(owner, dict) or not owner.get("run_contract_id"):
                raise RuntimeError("registered roll has no new run contract owner")
            registered_recovery_next_state = plan_registered_roll_restart(
                registered_recovery_state,
                now=now,
                round_id=(
                    f"competition-roll:{registered_recovery_state.symbol}:"
                    f"{owner['run_contract_id']}:{now.isoformat()}"
                ),
            )
            recovery_envelope = {
                "schema_version": RECOVERY_STATE_SCHEMA_VERSION,
                "state": JsonRecoveryStore.encode_state(
                    registered_recovery_next_state
                ),
            }
            next_desired_fields = decode_recovery_desired_profile_fields(
                recovery_envelope,
                expected_symbol=registered_recovery_state.symbol,
            )
            for key in registered_recovery_state.desired_profile.fields:
                updated.pop(key, None)
            updated.update(next_desired_fields)
            updated[RECOVERY_STATE_KEY] = recovery_envelope
            updated[RECOVERY_STATE_MIRROR_KEY] = recovery_envelope
            validated_next_state = decode_recovery_control_state(
                updated,
                expected_symbol=registered_recovery_state.symbol,
            )
            if validated_next_state != registered_recovery_next_state:
                raise RuntimeError("registered roll recovery state changed before write")
        changed = (
            updated != control
            or recovered_from is not None
            or target_changed
            or guard_baseline_changed
            or run_contract_owner_changed
        )
        if changed:
            write_json_atomically(control_path, updated)
    if loss_recovery_cleared and args.loop_state_path is not None and loop_state is not None:
        write_json_atomically(args.loop_state_path, loop_state)
    state_changed = False
    if (
        registered_recovery_state is None
        and changed
        and args.guard_state_path is not None
        and args.guard_state_path.exists()
    ):
        state = json.loads(args.guard_state_path.read_text(encoding="utf-8"))
        if isinstance(state, dict):
            state_changed = clear_recovery_overlay(state, symbol=str(args.symbol).upper())
            if state_changed:
                write_json_atomically(args.guard_state_path, state)
    print(
        json.dumps(
            {
                "changed": changed,
                "status": "applied",
                "run_start_time": updated.get("run_start_time"),
                "run_end_time": updated.get("run_end_time"),
                "runtime_guard_stats_start_time": updated.get("runtime_guard_stats_start_time"),
                "profile_rebased": bool(runtime_profile)
                and changed
                and not managed_profile_changes_deferred,
                "profile_rebase_partial": bool(runtime_profile)
                and changed
                and bool(managed_profile_changes_deferred),
                "recovery_overlay_cleared": state_changed,
                "target_notional": args.target_notional,
                "target_changed": target_changed,
                "runtime_guard_baseline_reset": guard_baseline_changed,
                "runtime_guard_loss_recovery_cleared": loss_recovery_cleared,
                "control_recovered_from": recovered_from,
                "registered_recovery_contract_roll": (
                    registered_recovery_state is not None
                ),
                "recovery_restart_scheduled": (
                    registered_recovery_next_state is not None
                ),
                "recovery_managed_profile_preserved": (
                    registered_recovery_state is not None
                ),
                "recovery_phase": (
                    (
                        registered_recovery_next_state
                        or registered_recovery_state
                    ).phase.value
                    if registered_recovery_state is not None
                    else None
                ),
                "recovery_effect_stage": (
                    registered_recovery_next_state.pending_effect_stage.value
                    if registered_recovery_next_state is not None
                    else None
                ),
                "managed_profile_changes_deferred": managed_profile_changes_deferred,
                "run_contract_owner_changed": run_contract_owner_changed,
                "run_contract_id": (
                    updated.get(RUN_CONTRACT_OWNER_KEY, {}).get("run_contract_id")
                    if isinstance(updated.get(RUN_CONTRACT_OWNER_KEY), dict)
                    else None
                ),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
