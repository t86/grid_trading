#!/usr/bin/env python3
"""Symbol-generic ledger auto-realign for hedge best-quote volume farming.

Tracked, deploy-via-git form of the hand-maintained ``output/ops/arx_auto_realign.py``.
Some profiles close one side through a non-BQ book (grid take-profit), so the BQ
ledger inflates structurally and reconcile never runs while orders stay active.
When ledger-vs-exchange drift exceeds the threshold this tool rewrites the ledger
lots to exchange truth. Run as::

    python -m grid_optimizer.competition_state_realign --symbol ARXUSDT \
        --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --enforce

Revival policy (the 2026-07-04 ARX incident fix): this tool NEVER revives a runner
it did not stop itself.

- Service ACTIVE + drift: stop -> realign -> start (the original mid-flight repair).
- Service INACTIVE + drift: realign the state ONLY and log ``skipped_start_inactive``.
  An inactive runner was stopped for a reason (runtime guard risk stop, target gate,
  manual) — blind restarts into a crash are how a 566 net blowout became 1198.
  ``--allow-start-when-stopped`` is the explicit escape hatch.

Startup latch fix: whenever a (re)start may follow a realign, the stale
``<symbol>_loop_latest_plan.json`` is archived first. The runtime guard reads that
file at startup, so a pre-realign snapshot with a huge net notional would trip
``max_actual_net_notional_hit`` before the first fresh cycle can replace it,
latching the runner stopped forever.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .audit import (
    exclusive_json_state_lock,
    fetch_time_paged,
    trade_row_key,
    trade_row_time_ms,
    write_json,
)
from .data import (
    delete_futures_order,
    fetch_futures_open_orders,
    fetch_futures_position_risk_v3,
    fetch_futures_user_trades,
)
from .futures_inventory_boundary import (
    durable_frozen_order_identities,
    ordinary_position_qtys,
    strict_frozen_side_qtys,
)
from .futures_recovery_store import recovery_coordinator_registered
from .futures_terminal_ownership import terminal_drain_completed_owner_is_integral
from .recovery_control_ownership import exclusive_control_lock


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def strategy_client_order_prefix(symbol: str) -> str:
    """The runner's managed-order clientOrderId prefix (e.g. ``gx-arxu-``).

    Must stay in sync with ``loop_runner._strategy_client_order_prefix`` (a test
    asserts parity). Realign only cancels orders under this prefix: protective
    resting orders like the target gate's ``FROZENTP*`` take-profit, manual
    orders, and external flatten orders are never touched.
    """
    return f"gx-{symbol.lower().replace('usdt', 'u')}-"


def is_managed_order(order: dict[str, Any], symbol: str) -> bool:
    return str(order.get("clientOrderId") or "").startswith(strategy_client_order_prefix(symbol))


def qty_sum(lots: Any) -> float:
    return sum(float(x.get("qty") or 0) for x in lots or [])


def is_active(service: str) -> bool:
    return subprocess.run(
        ["systemctl", "is-active", service], capture_output=True, text=True
    ).stdout.strip() == "active"


def fetch_exchange_sides(sym: str, k: str, s: str) -> tuple[float, float, float, float]:
    """Return (long_qty, long_entry, short_qty, short_entry) from position risk."""
    pos = fetch_futures_position_risk_v3(symbol=sym, api_key=k, api_secret=s)
    by = {p.get("positionSide"): p for p in pos}
    return (
        abs(float(by.get("LONG", {}).get("positionAmt") or 0)),
        float(by.get("LONG", {}).get("entryPrice") or 0),
        abs(float(by.get("SHORT", {}).get("positionAmt") or 0)),
        float(by.get("SHORT", {}).get("entryPrice") or 0),
    )


def compute_drift(state: dict[str, Any], long_qty: float, short_qty: float) -> tuple[float, float]:
    """Ordinary ledger minus ordinary exchange position, per side."""
    led = state.get("best_quote_volume_ledger") or {}
    fro = state.get("best_quote_frozen_inventory") or {}
    frozen_long_qty, frozen_short_qty = strict_frozen_side_qtys(fro)
    ordinary_long_qty, ordinary_short_qty = ordinary_position_qtys(
        exchange_long_qty=long_qty,
        exchange_short_qty=short_qty,
        frozen_long_qty=frozen_long_qty,
        frozen_short_qty=frozen_short_qty,
    )
    ldrift = qty_sum(led.get("long_lots")) - ordinary_long_qty
    sdrift = qty_sum(led.get("short_lots")) - ordinary_short_qty
    return ldrift, sdrift


def _best_quote_trade_fill_key(row: dict[str, Any]) -> str:
    order_id = str(row.get("orderId") or row.get("order_id") or "").strip()
    trade_id = str(row.get("id") or row.get("tradeId") or "").strip()
    if not trade_id.isdigit():
        return ""
    return f"{order_id}:trade:{trade_id}" if order_id else f"trade:{trade_id}"


def _seal_reflected_trade_rows(ledger: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    reflected = [dict(row) for row in rows if isinstance(row, dict) and trade_row_time_ms(row) > 0]
    if not reflected:
        return
    previous_time_ms = int(ledger.get("last_trade_time_ms") or 0)
    reflected_time_ms = max(trade_row_time_ms(row) for row in reflected)
    sealed_time_ms = max(previous_time_ms, reflected_time_ms)
    sealed_keys = {
        trade_row_key(row)
        for row in reflected
        if trade_row_time_ms(row) == sealed_time_ms
    }
    if sealed_time_ms == previous_time_ms:
        sealed_keys.update(str(item) for item in list(ledger.get("last_trade_keys_at_time") or []))
    applied_fill_keys = [
        str(item).strip()
        for item in list(ledger.get("applied_trade_fill_keys") or [])
        if str(item).strip()
    ]
    applied_fill_key_set = set(applied_fill_keys)
    for row in reflected:
        fill_key = _best_quote_trade_fill_key(row)
        if fill_key and fill_key not in applied_fill_key_set:
            applied_fill_keys.append(fill_key)
            applied_fill_key_set.add(fill_key)
    ledger["last_trade_time_ms"] = sealed_time_ms
    ledger["last_trade_keys_at_time"] = sorted(sealed_keys)
    ledger["applied_trade_fill_keys"] = applied_fill_keys[-10000:]
    ledger["realign_trade_cursor_sealed_at"] = _now_iso()
    ledger["realign_reflected_trade_count"] = len(reflected)


def fetch_settled_realign_snapshot(
    symbol: str,
    api_key: str,
    api_secret: str,
    *,
    start_time_ms: int,
    attempts: int = 5,
    settle_seconds: float = 0.25,
) -> tuple[float, float, float, float, list[dict[str, Any]]]:
    previous_marker: tuple[Any, ...] | None = None
    for _ in range(max(int(attempts), 2)):
        end_time_ms = int(time.time() * 1000)
        rows = fetch_time_paged(
            fetch_page=lambda **params: fetch_futures_user_trades(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                **params,
            ),
            start_time_ms=max(int(start_time_ms), 0),
            end_time_ms=end_time_ms,
            limit=1000,
            row_time_ms=trade_row_time_ms,
            row_key=trade_row_key,
        )
        lq, lavg, sq, savg = fetch_exchange_sides(symbol, api_key, api_secret)
        marker = (
            round(lq, 12),
            round(lavg, 12),
            round(sq, 12),
            round(savg, 12),
            tuple(sorted(trade_row_key(row) for row in rows)),
        )
        if marker == previous_marker:
            return lq, lavg, sq, savg, rows
        previous_marker = marker
        time.sleep(max(float(settle_seconds), 0.0))
    raise RuntimeError("exchange position/trade snapshot did not settle after managed-order cancel")


def realign_ledger(
    state: dict[str, Any],
    lq: float,
    lavg: float,
    sq: float,
    savg: float,
    *,
    reflected_trade_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Rewrite BQ ledger lots to exchange truth, preserving frozen inventory."""
    led = state.get("best_quote_volume_ledger") or {}
    fro = state.get("best_quote_frozen_inventory") or {}
    fro_l, fro_s = strict_frozen_side_qtys(fro)
    act_l, act_s = ordinary_position_qtys(
        exchange_long_qty=lq,
        exchange_short_qty=sq,
        frozen_long_qty=fro_l,
        frozen_short_qty=fro_s,
    )
    led["long_lots"] = (
        [{"qty": act_l, "price": lavg, "source": "auto_realign", "side": "LONG"}] if act_l > 0 else []
    )
    led["short_lots"] = (
        [{"qty": act_s, "price": savg, "source": "auto_realign", "side": "SHORT"}] if act_s > 0 else []
    )
    _seal_reflected_trade_rows(led, list(reflected_trade_rows or []))
    state["best_quote_volume_ledger"] = led
    return {"new_long": act_l, "new_short": act_s}


def repair_frozen_inventory_deficit(
    state: dict[str, Any], *, long_qty: float, short_qty: float
) -> dict[str, Any]:
    """Reconcile only frozen lots that no longer exist on the exchange.

    This is an explicit incident repair, not a release path: it never creates an
    exchange order or treats the removed quantity as a profitable/paired exit.
    Lots are retired newest-first and retained in audit history so the exchange
    remains the source of truth after an ordinary reducer has crossed the
    frozen boundary.
    """
    frozen = dict(state.get("best_quote_frozen_inventory") or {})
    exchange_qty = {"long": max(float(long_qty), 0.0), "short": max(float(short_qty), 0.0)}
    repaired: dict[str, Any] = {"long": [], "short": []}
    for side in ("long", "short"):
        key = f"{side}_lots"
        lots = [dict(item) for item in frozen.get(key, []) if isinstance(item, dict)]
        lot_qty = sum(max(float(item.get("qty") or 0.0), 0.0) for item in lots)
        aggregate_qty = max(float(frozen.get(f"{side}_qty") or 0.0), 0.0)
        active_qty = max(lot_qty, aggregate_qty)
        deficit = max(active_qty - exchange_qty[side], 0.0)
        if deficit <= 1e-12:
            continue
        kept_reversed: list[dict[str, Any]] = []
        for lot in reversed(lots):
            qty = max(float(lot.get("qty") or 0.0), 0.0)
            removed = min(qty, deficit)
            if removed > 0:
                retired = dict(lot)
                retired["qty"] = removed
                retired["retired_reason"] = "exchange_position_deficit_reconcile"
                repaired[side].append(retired)
                qty -= removed
                deficit -= removed
            if qty > 1e-12:
                retained = dict(lot)
                retained["qty"] = qty
                kept_reversed.append(retained)
        kept = list(reversed(kept_reversed))
        frozen[key] = kept
        frozen[f"{side}_qty"] = sum(max(float(item.get("qty") or 0.0), 0.0) for item in kept)
    if any(repaired.values()):
        history = list(frozen.get("exchange_deficit_reconcile_history") or [])
        history.append({"at": _now_iso(), "reason": "exchange_position_deficit_reconcile", "retired": repaired})
        frozen["exchange_deficit_reconcile_history"] = history[-20:]
        frozen["updated_at"] = _now_iso()
        state["best_quote_frozen_inventory"] = frozen
    return {
        "repaired": bool(any(repaired.values())),
        "retired_long_qty": sum(float(item.get("qty") or 0.0) for item in repaired["long"]),
        "retired_short_qty": sum(float(item.get("qty") or 0.0) for item in repaired["short"]),
        "retired": repaired,
    }


def archive_stale_plan(workdir: str, slug: str) -> str | None:
    """Move the persisted latest-plan snapshot aside so a following start cannot
    latch on its pre-realign net notional (guard reads it before the first fresh
    cycle). The runner rewrites the file on its first cycle."""
    plan_path = os.path.join(workdir, "output", f"{slug}_loop_latest_plan.json")
    if not os.path.exists(plan_path):
        return None
    dst = plan_path + ".stale_realign_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    try:
        os.replace(plan_path, dst)
        return dst
    except OSError:
        return None


def should_start_after_realign(was_active: bool, allow_start_when_stopped: bool) -> bool:
    """Only revive a runner this tool stopped itself (it was active). An already
    inactive runner was stopped for a reason (risk guard / target gate / manual);
    restarting it into the same market is how the 2026-07-04 net blowout compounded."""
    return was_active or allow_start_when_stopped


def completed_terminal_resume_eligibility(
    state: dict[str, Any],
    *,
    symbol: str,
    now: datetime,
) -> tuple[bool, str]:
    """Validate the narrow operator-approved recovery from a stale drain owner.

    Terminal drain is normally irreversible for a run contract.  This escape
    hatch is intentionally limited to an already-completed ``condition_unmet``
    owner while its volume target and time window are still live.  It never
    applies to a deadline or completed-target exit.
    """
    owner = state.get("futures_terminal_drain")
    if not isinstance(owner, dict):
        return False, "terminal_drain_missing"
    if not terminal_drain_completed_owner_is_integral(
        owner, symbol=symbol, loop_state=state
    ):
        return False, "terminal_drain_not_completed_or_invalid"
    if str(owner.get("run_outcome") or "") != "condition_unmet":
        return False, "terminal_drain_outcome_not_condition_unmet"
    target = float(owner.get("target_value") or 0.0)
    achieved = float(owner.get("achieved_value") or 0.0)
    if target <= 0 or achieved >= target - 1e-9:
        return False, "terminal_drain_target_not_unmet"
    snapshot = owner.get("run_contract_snapshot")
    if not isinstance(snapshot, dict):
        return False, "terminal_drain_snapshot_missing"
    try:
        run_start = datetime.fromisoformat(
            str(snapshot["run_start_time"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        run_end = datetime.fromisoformat(
            str(snapshot["run_end_time"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except (KeyError, TypeError, ValueError):
        return False, "terminal_drain_window_invalid"
    checked_at = now.astimezone(timezone.utc)
    if checked_at < run_start or checked_at >= run_end:
        return False, "terminal_drain_window_not_active"
    if list(owner.get("active_intent_ids") or []):
        return False, "terminal_drain_order_in_flight"
    return True, "eligible"


def archive_completed_terminal_for_active_target(
    state: dict[str, Any],
    *,
    symbol: str,
    now: datetime,
) -> dict[str, Any]:
    """Archive a validated completed drain without touching inventory ledgers."""
    eligible, reason = completed_terminal_resume_eligibility(
        state, symbol=symbol, now=now
    )
    if not eligible:
        raise ValueError(reason)
    owner = dict(state.pop("futures_terminal_drain"))
    owner.update(
        {
            "archived_at": now.astimezone(timezone.utc).isoformat(),
            "archive_reason": "explicit_resume_active_unmet_target",
        }
    )
    history = list(state.get("futures_terminal_drain_history") or [])
    history.append(owner)
    state["futures_terminal_drain_history"] = history[-20:]
    state.pop("futures_terminal_handoff", None)
    state["terminal_drain_resume"] = {
        "symbol": symbol,
        "at": now.astimezone(timezone.utc).isoformat(),
        "reason": "explicit_resume_active_unmet_target",
        "previous_decision_id": owner.get("decision_id"),
        "target_value": owner.get("target_value"),
        "achieved_value": owner.get("achieved_value"),
    }
    return owner


def _load_recovery_control(path: Path) -> tuple[dict[str, Any], bool]:
    """Return a control document and whether its ownership can be determined.

    A missing control document is an ordinary legacy-runner case.  A present
    document that cannot be decoded (or is not an object) has unknown owner and
    must never authorize this legacy executor to stop a service or mutate state.
    """
    try:
        with path.open(encoding="utf-8") as handle:
            control = json.load(handle)
    except FileNotFoundError:
        return {}, True
    except (json.JSONDecodeError, OSError):
        return {}, False
    return (control, True) if isinstance(control, dict) else ({}, False)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--service", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--threshold-qty", type=float, default=150.0)
    ap.add_argument("--allow-start-when-stopped", action="store_true",
                    help="Escape hatch: also start the service when it was already "
                         "inactive. Default off — an inactive runner stays down.")
    ap.add_argument(
        "--resume-completed-active-target",
        action="store_true",
        help="Archive only a completed condition_unmet terminal drain while its "
        "target window remains active; requires --enforce and no terminal order.",
    )
    ap.add_argument(
        "--repair-frozen-inventory-deficit",
        action="store_true",
        help="Explicitly retire only frozen ledger quantity absent from exchange; requires --enforce.",
    )
    ap.add_argument("--enforce", action="store_true")
    return ap


def main() -> None:
    a = build_parser().parse_args()
    sym = a.symbol.upper()
    slug = sym.lower()
    k = os.environ["BINANCE_API_KEY"]
    s = os.environ["BINANCE_API_SECRET"]
    state_path = os.path.join(a.workdir, "output", f"{slug}_loop_state.json")
    control_path = os.path.join(
        a.workdir,
        "output",
        f"{slug}_loop_runner_control.json",
    )
    control, control_readable = _load_recovery_control(Path(control_path))
    coordinator_registered = recovery_coordinator_registered(control)

    lq, lavg, sq, savg = fetch_exchange_sides(sym, k, s)
    state = json.load(open(state_path))
    try:
        ldrift, sdrift = compute_drift(state, lq, sq)
    except ValueError:
        if not (a.repair_frozen_inventory_deficit and a.enforce):
            raise
        ldrift = sdrift = 0.0
    was_active = is_active(a.service)
    status: dict[str, Any] = {
        "ts": _now_iso(), "symbol": sym, "long_drift": round(ldrift, 1),
        "short_drift": round(sdrift, 1), "exch_long": lq, "exch_short": sq,
        "threshold": a.threshold_qty, "was_active": was_active,
        "recovery_coordinator_registered": coordinator_registered,
        "recovery_coordinator_ownership_unknown": not control_readable,
    }
    if a.resume_completed_active_target and not a.enforce:
        status["action"] = "DRY_RUN_would_resume_completed_active_target"
        print(json.dumps(status))
        return
    if (
        not a.resume_completed_active_target
        and (max(abs(ldrift), abs(sdrift)) <= a.threshold_qty or not a.enforce)
    ):
        status["action"] = (
            "none" if max(abs(ldrift), abs(sdrift)) <= a.threshold_qty else "DRY_RUN_would_realign"
        )
        print(json.dumps(status))
        return

    if coordinator_registered or not control_readable:
        status["action"] = (
            "DEFERRED_TO_FUTURES_RECOVERY_COORDINATOR"
            if coordinator_registered
            else "BLOCKED_UNREADABLE_RECOVERY_CONTROL"
        )
        status["requested_action"] = "REALIGN_LEDGER"
        print(json.dumps(status))
        return

    # The runner, Web frozen actions and legacy repair tools share one state
    # document.  Use the same state -> control order as the runner/Web, then
    # recheck coordinator ownership before stopping or mutating anything.
    with exclusive_json_state_lock(Path(state_path)):
        with exclusive_control_lock(Path(control_path)):
            locked_control, locked_control_readable = _load_recovery_control(
                Path(control_path)
            )
            locked_coordinator_registered = recovery_coordinator_registered(locked_control)
            if locked_coordinator_registered or not locked_control_readable:
                status["recovery_coordinator_registered"] = locked_coordinator_registered
                status["recovery_coordinator_ownership_unknown"] = not locked_control_readable
                status["action"] = (
                    "DEFERRED_TO_FUTURES_RECOVERY_COORDINATOR"
                    if status["recovery_coordinator_registered"]
                    else "BLOCKED_UNREADABLE_RECOVERY_CONTROL"
                )
                status["requested_action"] = "REALIGN_LEDGER"
                print(json.dumps(status))
                return

            with open(state_path, encoding="utf-8") as state_handle:
                locked_state = json.load(state_handle)
            if a.repair_frozen_inventory_deficit:
                if not a.enforce:
                    status["action"] = "DRY_RUN_would_repair_frozen_inventory_deficit"
                    print(json.dumps(status))
                    return
                bak = state_path + ".bak_frozen_deficit_" + str(int(time.time()))
                try:
                    shutil.copy2(state_path, bak)
                except OSError as exc:
                    status["action"] = "ABORTED_BACKUP_FAILED"
                    status["error"] = str(exc)[:140]
                    print(json.dumps(status))
                    raise SystemExit(1)
                repair = repair_frozen_inventory_deficit(
                    locked_state, long_qty=lq, short_qty=sq
                )
                if not repair["repaired"]:
                    status["action"] = "FROZEN_INVENTORY_DEFICIT_NOT_PRESENT"
                    print(json.dumps(status))
                    return
                ldrift, sdrift = compute_drift(locked_state, lq, sq)
                new_lots = realign_ledger(locked_state, lq, lavg, sq, savg)
                locked_state["updated_at"] = _now_iso()
                locked_state["updated_by"] = "competition_state_realign_frozen_deficit"
                locked_state["last_realign"] = {"at": _now_iso(), "backup": bak, **repair, **new_lots}
                write_json(Path(state_path), locked_state)
                status.update({"action": "FROZEN_INVENTORY_DEFICIT_REALIGNED", "backup": bak, **repair, **new_lots})
                archived = archive_stale_plan(a.workdir, slug)
                if archived:
                    status["stale_plan_archived"] = archived
                if should_start_after_realign(was_active, a.allow_start_when_stopped):
                    start = subprocess.run(["sudo", "-n", "systemctl", "start", a.service], capture_output=True)
                    status["start_rc"] = start.returncode
                print(json.dumps(status))
                return
            if a.resume_completed_active_target:
                if is_active(a.service):
                    status["action"] = "RESUME_COMPLETED_ACTIVE_TARGET_BLOCKED"
                    status["reason"] = "runner_still_active"
                    print(json.dumps(status))
                    return
                now = datetime.now(timezone.utc)
                eligible, reason = completed_terminal_resume_eligibility(
                    locked_state, symbol=sym, now=now
                )
                if not eligible:
                    status["action"] = "RESUME_COMPLETED_ACTIVE_TARGET_BLOCKED"
                    status["reason"] = reason
                    print(json.dumps(status))
                    return
                current_orders = fetch_futures_open_orders(sym, k, s) or []
                if current_orders:
                    status["action"] = "RESUME_COMPLETED_ACTIVE_TARGET_BLOCKED"
                    status["reason"] = "open_orders_present"
                    status["open_order_count"] = len(current_orders)
                    print(json.dumps(status))
                    return
                archived_owner = archive_completed_terminal_for_active_target(
                    locked_state, symbol=sym, now=now
                )
                locked_state["updated_at"] = _now_iso()
                locked_state["updated_by"] = "competition_state_realign_resume"
                write_json(Path(state_path), locked_state)
                status.update(
                    {
                        "action": "RESUMED_COMPLETED_ACTIVE_TARGET",
                        "archived_terminal_decision_id": archived_owner.get("decision_id"),
                    }
                )
                archived = archive_stale_plan(a.workdir, slug)
                if archived:
                    status["stale_plan_archived"] = archived
                print(json.dumps(status))
                return
            ldrift, sdrift = compute_drift(locked_state, lq, sq)
            status["long_drift"] = round(ldrift, 1)
            status["short_drift"] = round(sdrift, 1)
            if max(abs(ldrift), abs(sdrift)) <= a.threshold_qty:
                status["action"] = "none"
                print(json.dumps(status))
                return

            if was_active:
                subprocess.run(
                    ["sudo", "-n", "systemctl", "stop", a.service],
                    capture_output=True,
                )
                time.sleep(2)

            # Backup must succeed before any exchange or state mutation.
            bak = state_path + ".bak_autorealign_" + str(int(time.time()))
            try:
                shutil.copy2(state_path, bak)
            except OSError as exc:
                status["action"] = "ABORTED_BACKUP_FAILED"
                status["error"] = str(exc)[:140]
                print(json.dumps(status))
                raise SystemExit(1)

            frozen_order_ids, frozen_client_order_ids = durable_frozen_order_identities(
                locked_state
            )

            # Cancel only ordinary runner-managed orders.  Frozen ownership
            # comes from exact durable refs/manifest identities, never a prefix.
            canceled = skipped_protected = kept_frozen = 0
            for o in fetch_futures_open_orders(sym, k, s) or []:
                order_id = str(o.get("orderId") or o.get("order_id") or "").strip()
                client_order_id = str(
                    o.get("clientOrderId") or o.get("client_order_id") or ""
                ).strip()
                if order_id in frozen_order_ids or client_order_id in frozen_client_order_ids:
                    kept_frozen += 1
                    continue
                if not is_managed_order(o, sym):
                    skipped_protected += 1
                    continue
                try:
                    delete_futures_order(
                        symbol=sym,
                        order_id=o["orderId"],
                        api_key=k,
                        api_secret=s,
                    )
                    canceled += 1
                except Exception:
                    pass
            status["canceled_managed_orders"] = canceled
            status["kept_frozen_orders"] = kept_frozen
            status["kept_unmanaged_orders"] = skipped_protected

            ledger = locked_state.get("best_quote_volume_ledger") or {}
            lq, lavg, sq, savg, reflected_trade_rows = fetch_settled_realign_snapshot(
                sym,
                k,
                s,
                start_time_ms=int(ledger.get("last_trade_time_ms") or 0),
            )
            new_lots = realign_ledger(
                locked_state,
                lq,
                lavg,
                sq,
                savg,
                reflected_trade_rows=reflected_trade_rows,
            )
            locked_state["updated_at"] = _now_iso()
            locked_state["updated_by"] = "competition_state_realign"
            locked_state["last_realign"] = {
                "at": _now_iso(),
                "backup": bak,
                **new_lots,
            }
            write_json(Path(state_path), locked_state)

            status.update(
                {
                    "backup": bak,
                    "reflected_trade_count": len(reflected_trade_rows),
                    **new_lots,
                }
            )
            archived = archive_stale_plan(a.workdir, slug)
            if archived:
                status["stale_plan_archived"] = archived
            if should_start_after_realign(was_active, a.allow_start_when_stopped):
                start = subprocess.run(
                    ["sudo", "-n", "systemctl", "start", a.service],
                    capture_output=True,
                )
                status["action"] = "REALIGNED_AND_RESTARTED"
                status["start_rc"] = start.returncode
            else:
                # Never revive a runner this tool did not stop itself.
                status["action"] = "REALIGNED_SKIPPED_START_INACTIVE"
    print(json.dumps(status))


if __name__ == "__main__":
    main()
