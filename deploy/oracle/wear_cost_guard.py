#!/usr/bin/env python3
"""Throttle saved runners when wear cost per 10k gross notional gets too high."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GuardTier:
    name: str
    trigger_cost_per_10k: float
    per_order_scale: float
    exposure_scale: float
    max_new_orders_scale: float
    sleep_floor_seconds: float


TIERS = [
    GuardTier("normal", 0.0, 1.0, 1.0, 1.0, 0.0),
    GuardTier("watch", 0.30, 0.8, 0.75, 0.75, 3.0),
    GuardTier("tight", 0.40, 0.6, 0.55, 0.55, 4.0),
    GuardTier("emergency", 0.48, 0.4, 0.35, 0.35, 5.0),
]

BASELINE_KEYS = (
    "per_order_notional",
    "maker_order_notional",
    "max_total_notional",
    "pause_buy_position_notional",
    "pause_short_position_notional",
    "max_position_notional",
    "max_short_position_notional",
    "max_actual_net_notional",
    "max_synthetic_drift_notional",
    "threshold_position_notional",
    "hard_loss_forced_reduce_target_notional",
    "hard_loss_forced_reduce_max_order_notional",
    "adverse_reduce_max_order_notional",
    "max_new_orders",
    "sleep_seconds",
)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _latest_event(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            end = handle.tell()
            chunk_size = 8192
            data = b""
            while end > 0:
                read_size = min(chunk_size, end)
                end -= read_size
                handle.seek(end)
                data = handle.read(read_size) + data
                lines = [line for line in data.splitlines() if line.strip()]
                if len(lines) >= 2 or end == 0:
                    for raw_line in reversed(lines):
                        try:
                            item = json.loads(raw_line.decode("utf-8"))
                        except json.JSONDecodeError:
                            continue
                        if isinstance(item, dict):
                            return item
                    break
    except FileNotFoundError:
        return {}
    return {}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _select_tier(cost_per_10k: float) -> GuardTier:
    selected = TIERS[0]
    for tier in TIERS:
        if cost_per_10k >= tier.trigger_cost_per_10k:
            selected = tier
    return selected


def _baseline_for(symbol: str, control: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    baselines = state.setdefault("baselines", {})
    baseline = baselines.get(symbol)
    if isinstance(baseline, dict) and baseline:
        return baseline
    baseline = {key: control.get(key) for key in BASELINE_KEYS if key in control}
    baselines[symbol] = baseline
    return baseline


def _scaled_float(baseline: dict[str, Any], key: str, scale: float, *, minimum: float | None = None) -> float | None:
    if key not in baseline or baseline.get(key) is None:
        return None
    value = _as_float(baseline.get(key))
    if value <= 0:
        return value
    scaled = value * scale
    if minimum is not None:
        scaled = max(minimum, scaled)
    return round(scaled, 8)


def _apply_tier(control: dict[str, Any], baseline: dict[str, Any], tier: GuardTier) -> tuple[dict[str, Any], list[str]]:
    updated = dict(control)
    changes: list[str] = []

    numeric_scales = {
        "per_order_notional": (tier.per_order_scale, 15.0),
        "maker_order_notional": (tier.per_order_scale, 15.0),
        "max_total_notional": (tier.exposure_scale, 100.0),
        "pause_buy_position_notional": (tier.exposure_scale, 30.0),
        "pause_short_position_notional": (tier.exposure_scale, 30.0),
        "max_position_notional": (tier.exposure_scale, 50.0),
        "max_short_position_notional": (tier.exposure_scale, 50.0),
        "max_actual_net_notional": (tier.exposure_scale, 30.0),
        "max_synthetic_drift_notional": (tier.exposure_scale, 20.0),
        "threshold_position_notional": (tier.exposure_scale, 0.0),
        "hard_loss_forced_reduce_target_notional": (tier.exposure_scale, 30.0),
        "hard_loss_forced_reduce_max_order_notional": (tier.per_order_scale, 15.0),
        "adverse_reduce_max_order_notional": (tier.per_order_scale, 0.0),
    }
    for key, (scale, minimum) in numeric_scales.items():
        scaled = _scaled_float(baseline, key, scale, minimum=minimum)
        if scaled is None:
            continue
        if updated.get(key) != scaled:
            updated[key] = scaled
            changes.append(key)

    if "max_new_orders" in baseline and baseline.get("max_new_orders") is not None:
        baseline_orders = max(1, _as_int(baseline.get("max_new_orders"), 1))
        scaled_orders = max(4 if tier.name == "emergency" else 6, int(round(baseline_orders * tier.max_new_orders_scale)))
        if tier.name == "normal":
            scaled_orders = baseline_orders
        if updated.get("max_new_orders") != scaled_orders:
            updated["max_new_orders"] = scaled_orders
            changes.append("max_new_orders")

    if "sleep_seconds" in baseline and baseline.get("sleep_seconds") is not None:
        baseline_sleep = _as_float(baseline.get("sleep_seconds"))
        sleep_seconds = max(baseline_sleep, tier.sleep_floor_seconds)
        if updated.get("sleep_seconds") != sleep_seconds:
            updated["sleep_seconds"] = sleep_seconds
            changes.append("sleep_seconds")

    updated["apply"] = True
    return updated, changes


def _append_audit(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _restart_symbol(symbol: str, *, wrapper: str) -> None:
    subprocess.run([wrapper, "restart", symbol], check=True)


def run_guard(args: argparse.Namespace) -> int:
    app_dir = Path(args.app_dir).resolve()
    output_dir = app_dir / "output"
    state_path = output_dir / "wear_cost_guard_state.json"
    audit_path = output_dir / "wear_cost_guard_events.jsonl"
    state = _read_json(state_path)
    state.setdefault("baselines", {})
    state.setdefault("last_tiers", {})

    now = datetime.now(timezone.utc).isoformat()
    exit_code = 0
    for symbol in args.symbols:
        symbol = symbol.upper().strip()
        slug = symbol.lower()
        control_path = output_dir / f"{slug}_loop_runner_control.json"
        events_path = output_dir / f"{slug}_loop_events.jsonl"
        control = _read_json(control_path)
        event = _latest_event(events_path)
        if not control or not event:
            row = {"ts": now, "symbol": symbol, "status": "skipped", "reason": "missing_control_or_event"}
            _append_audit(audit_path, row)
            print(json.dumps(row, ensure_ascii=False))
            exit_code = 1
            continue

        rolling_loss = max(0.0, _as_float(event.get("rolling_hourly_loss")))
        gross_notional = _as_float(event.get("cumulative_gross_notional"))
        cost_per_10k = (rolling_loss * 10000.0 / gross_notional) if gross_notional > 0 else 0.0
        tier = _select_tier(cost_per_10k)
        baseline = _baseline_for(symbol, control, state)
        updated, changes = _apply_tier(control, baseline, tier)
        previous_tier = str(state.get("last_tiers", {}).get(symbol) or "")
        should_restart = bool(changes) or previous_tier != tier.name

        row = {
            "ts": now,
            "symbol": symbol,
            "status": "ok",
            "tier": tier.name,
            "previous_tier": previous_tier or None,
            "rolling_hourly_loss": rolling_loss,
            "cumulative_gross_notional": gross_notional,
            "cost_per_10k": cost_per_10k,
            "changes": changes,
            "restart": should_restart and not args.dry_run,
            "dry_run": args.dry_run,
        }
        print(json.dumps(row, ensure_ascii=False, sort_keys=True))
        _append_audit(audit_path, row)
        state["last_tiers"][symbol] = tier.name

        if changes and not args.dry_run:
            backup_path = control_path.with_suffix(control_path.suffix + f".bak_wear_guard_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
            backup_path.write_text(json.dumps(control, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            _write_json(control_path, updated)
        if should_restart and not args.dry_run:
            try:
                _restart_symbol(symbol, wrapper=args.runner_wrapper)
            except subprocess.CalledProcessError as exc:
                row = {"ts": now, "symbol": symbol, "status": "restart_failed", "returncode": exc.returncode}
                _append_audit(audit_path, row)
                print(json.dumps(row, ensure_ascii=False), file=sys.stderr)
                exit_code = 1

    if not args.dry_run:
        state["updated_at"] = now
        _write_json(state_path, state)
    return exit_code


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-dir", default="/home/ubuntu/wangge")
    parser.add_argument("--runner-wrapper", default="/usr/local/bin/grid-saved-runner")
    parser.add_argument("--symbols", nargs="+", default=["BZUSDT", "CLUSDT"])
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    raise SystemExit(run_guard(args))


if __name__ == "__main__":
    main()
