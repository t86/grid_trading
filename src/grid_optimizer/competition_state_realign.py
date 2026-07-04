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
import subprocess
import time
from datetime import datetime, timezone
from typing import Any

from .data import (
    delete_futures_order,
    fetch_futures_open_orders,
    fetch_futures_position_risk_v3,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    """Ledger+frozen minus exchange, per side."""
    led = state.get("best_quote_volume_ledger") or {}
    fro = state.get("best_quote_frozen_inventory") or {}
    ldrift = qty_sum(led.get("long_lots")) + qty_sum(fro.get("long_lots")) - long_qty
    sdrift = qty_sum(led.get("short_lots")) + qty_sum(fro.get("short_lots")) - short_qty
    return ldrift, sdrift


def realign_ledger(state: dict[str, Any], lq: float, lavg: float, sq: float, savg: float) -> dict[str, Any]:
    """Rewrite BQ ledger lots to exchange truth, preserving frozen inventory."""
    led = state.get("best_quote_volume_ledger") or {}
    fro = state.get("best_quote_frozen_inventory") or {}
    fro_l = qty_sum(fro.get("long_lots"))
    fro_s = qty_sum(fro.get("short_lots"))
    act_l = max(lq - fro_l, 0.0)
    act_s = max(sq - fro_s, 0.0)
    led["long_lots"] = (
        [{"qty": act_l, "price": lavg, "source": "auto_realign", "side": "LONG"}] if act_l > 0 else []
    )
    led["short_lots"] = (
        [{"qty": act_s, "price": savg, "source": "auto_realign", "side": "SHORT"}] if act_s > 0 else []
    )
    state["best_quote_volume_ledger"] = led
    return {"new_long": act_l, "new_short": act_s}


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


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--service", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--threshold-qty", type=float, default=150.0)
    ap.add_argument("--allow-start-when-stopped", action="store_true",
                    help="Escape hatch: also start the service when it was already "
                         "inactive. Default off — an inactive runner stays down.")
    ap.add_argument("--enforce", action="store_true")
    return ap


def main() -> None:
    a = build_parser().parse_args()
    sym = a.symbol.upper()
    slug = sym.lower()
    k = os.environ["BINANCE_API_KEY"]
    s = os.environ["BINANCE_API_SECRET"]
    state_path = os.path.join(a.workdir, "output", f"{slug}_loop_state.json")

    lq, lavg, sq, savg = fetch_exchange_sides(sym, k, s)
    state = json.load(open(state_path))
    ldrift, sdrift = compute_drift(state, lq, sq)
    was_active = is_active(a.service)
    status: dict[str, Any] = {
        "ts": _now_iso(), "symbol": sym, "long_drift": round(ldrift, 1),
        "short_drift": round(sdrift, 1), "exch_long": lq, "exch_short": sq,
        "threshold": a.threshold_qty, "was_active": was_active,
    }
    if max(abs(ldrift), abs(sdrift)) <= a.threshold_qty or not a.enforce:
        status["action"] = (
            "none" if max(abs(ldrift), abs(sdrift)) <= a.threshold_qty else "DRY_RUN_would_realign"
        )
        print(json.dumps(status))
        return

    if was_active:
        subprocess.run(["sudo", "-n", "systemctl", "stop", a.service], capture_output=True)
        time.sleep(2)

    bak = state_path + ".bak_autorealign_" + str(int(time.time()))
    subprocess.run(["cp", state_path, bak])
    for o in fetch_futures_open_orders(sym, k, s) or []:
        try:
            delete_futures_order(symbol=sym, order_id=o["orderId"], api_key=k, api_secret=s)
        except Exception:
            pass

    lq, lavg, sq, savg = fetch_exchange_sides(sym, k, s)  # refresh after cancels
    state = json.load(open(state_path))
    new_lots = realign_ledger(state, lq, lavg, sq, savg)
    tmp = state_path + ".tmp"
    json.dump(state, open(tmp, "w"))
    os.replace(tmp, state_path)

    status.update({"backup": bak, **new_lots})
    if should_start_after_realign(was_active, a.allow_start_when_stopped):
        archived = archive_stale_plan(a.workdir, slug)
        if archived:
            status["stale_plan_archived"] = archived
        start = subprocess.run(["sudo", "-n", "systemctl", "start", a.service], capture_output=True)
        status["action"] = "REALIGNED_AND_RESTARTED"
        status["start_rc"] = start.returncode
    else:
        # Do NOT revive a runner that was already stopped: the stop had a reason
        # (risk guard / target gate / manual). Realigned books + archived plan mean
        # the eventual deliberate start is clean.
        archived = archive_stale_plan(a.workdir, slug)
        if archived:
            status["stale_plan_archived"] = archived
        status["action"] = "REALIGNED_SKIPPED_START_INACTIVE"
    print(json.dumps(status))


if __name__ == "__main__":
    main()
