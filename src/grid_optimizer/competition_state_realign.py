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
from typing import Any

from .audit import fetch_time_paged, trade_row_key, trade_row_time_ms
from .data import (
    delete_futures_order,
    fetch_futures_open_orders,
    fetch_futures_position_risk_v3,
    fetch_futures_user_trades,
)


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
    """Ledger+frozen minus exchange, per side."""
    led = state.get("best_quote_volume_ledger") or {}
    fro = state.get("best_quote_frozen_inventory") or {}
    ldrift = qty_sum(led.get("long_lots")) + qty_sum(fro.get("long_lots")) - long_qty
    sdrift = qty_sum(led.get("short_lots")) + qty_sum(fro.get("short_lots")) - short_qty
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
    _seal_reflected_trade_rows(led, list(reflected_trade_rows or []))
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

    # Backup must succeed before any mutation: abort the whole realign otherwise.
    bak = state_path + ".bak_autorealign_" + str(int(time.time()))
    try:
        shutil.copy2(state_path, bak)
    except OSError as exc:
        status["action"] = "ABORTED_BACKUP_FAILED"
        status["error"] = str(exc)[:140]
        print(json.dumps(status))
        raise SystemExit(1)

    # Cancel ONLY the runner's managed (gx-<sym>-) orders. Protective resting orders
    # (target gate FROZENTP*), manual and external orders stay untouched.
    canceled = skipped_protected = 0
    for o in fetch_futures_open_orders(sym, k, s) or []:
        if not is_managed_order(o, sym):
            skipped_protected += 1
            continue
        try:
            delete_futures_order(symbol=sym, order_id=o["orderId"], api_key=k, api_secret=s)
            canceled += 1
        except Exception:
            pass
    status["canceled_managed_orders"] = canceled
    status["kept_unmanaged_orders"] = skipped_protected

    state = json.load(open(state_path))
    ledger = state.get("best_quote_volume_ledger") or {}
    lq, lavg, sq, savg, reflected_trade_rows = fetch_settled_realign_snapshot(
        sym,
        k,
        s,
        start_time_ms=int(ledger.get("last_trade_time_ms") or 0),
    )
    new_lots = realign_ledger(
        state,
        lq,
        lavg,
        sq,
        savg,
        reflected_trade_rows=reflected_trade_rows,
    )
    state["updated_at"] = _now_iso()
    state["updated_by"] = "competition_state_realign"
    state["last_realign"] = {"at": _now_iso(), "backup": bak, **new_lots}
    tmp = state_path + ".tmp"
    json.dump(state, open(tmp, "w"))
    os.replace(tmp, state_path)

    status.update({"backup": bak, "reflected_trade_count": len(reflected_trade_rows), **new_lots})
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
