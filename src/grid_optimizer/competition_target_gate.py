#!/usr/bin/env python3
"""Symbol-generic server-side target gate for hedge best-quote volume farming.

This is the tracked, deploy-via-git form of the hand-maintained
``output/ops/<symbol>_target_gate.py`` scripts (REUSDT / ARXUSDT / OUSDT were
byte-identical apart from the symbol and file paths). Run it as::

    python -m grid_optimizer.competition_target_gate --symbol ARXUSDT \
        --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --enforce

Behaviour (unchanged from the battle-tested server version):

1. Read the real day volume / wear from exchange ``userTrades``
   (``-realized_pnl / gross_notional * 1e4`` = wear per 10k).
2. Stop + flatten the runner when the day volume hits the target
   (``max_cumulative_notional`` from the live control JSON) OR when volume is
   past ``--first`` and wear is above ``--wear-stop``.
3. Flatten ONLY the managed (non-frozen) position; frozen long/short inventory
   is kept so a stopped runner never realizes the frozen legs at a loss.
4. Rest a BUY take-profit LIMIT on the kept frozen shorts, priced so the whole
   frozen book closes at >= ``--tp-ratio`` average profit and no individual lot
   is underwater at the fill (``min_entry`` guard). The runner cancels this
   ``FROZENTP*`` order on its next active cycle.

A per-symbol ``<symbol>_target_gate_done_YYYYMMDD.flag`` under ``output/``
prevents repeating the stop/flatten within the same UTC day.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data import (
    delete_futures_order,
    fetch_futures_open_orders,
    fetch_futures_position_risk_v3,
    fetch_futures_user_trades,
    post_futures_market_order,
    post_futures_order,
)
from .monitor import summarize_user_trades

FROZEN_TP_PREFIX = "FROZENTP"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def daily_vol_wear(sym: str, k: str, s: str) -> tuple[float, float]:
    """Return (day gross notional, day wear per 10k) from exchange userTrades."""
    now = _now()
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    cur = int(start.timestamp() * 1000)
    trades: list[dict[str, Any]] = []
    seen: set[Any] = set()
    pages = 0
    while pages < 80:
        pages += 1
        batch = fetch_futures_user_trades(symbol=sym, api_key=k, api_secret=s, start_time_ms=cur, limit=1000)
        new = [t for t in batch if t.get("id") not in seen]
        for t in new:
            seen.add(t.get("id"))
        trades += new
        if len(batch) < 1000 or not new:
            break
        cur = max(int(t.get("time", 0)) for t in batch)
    su = summarize_user_trades(trades)
    v = su["gross_notional"]
    r = su["realized_pnl"]
    return v, ((-r / v * 1e4) if v > 0 else 0.0)


def _frozen_short_lots(workdir: str, slug: str) -> list[tuple[float, float]]:
    try:
        st = json.load(open(os.path.join(workdir, "output", f"{slug}_loop_state.json")))
        sl = (st.get("best_quote_frozen_inventory") or {}).get("short_lots") or []
        out: list[tuple[float, float]] = []
        for lot in sl:
            q = float(lot.get("qty", 0) or 0)
            e = float(lot.get("entry_price", lot.get("price", 0)) or 0)
            if q > 0 and e > 0:
                out.append((q, e))
        return out
    except Exception:
        return []


def place_frozen_short_tp(
    sym: str, k: str, s: str, workdir: str, slug: str, tp_ratio: float, tick: float
) -> dict[str, Any]:
    """Rest a BUY limit to take profit on kept frozen shorts.

    Price = ``min(wavg * (1 - tp_ratio), min_entry)`` floored to tick: the whole
    book closes at >= ``tp_ratio`` average profit AND no individual lot is
    underwater at the fill price (min_entry guard), so a stopped runner can never
    realize a loss on the kept frozen shorts.
    """
    lots = _frozen_short_lots(workdir, slug)
    if not lots:
        return {"placed": False, "reason": "no_frozen_short_lots"}
    tot_q = sum(q for q, _ in lots)
    tot_c = sum(q * e for q, e in lots)
    if tot_q <= 0:
        return {"placed": False, "reason": "zero_qty"}
    wavg = tot_c / tot_q
    min_entry = min(e for _, e in lots)
    raw = min(wavg * (1.0 - max(tp_ratio, 0.0)), min_entry)
    tp_price = round(math.floor(raw / tick) * tick, 8)
    qty = int(tot_q)
    if qty <= 0 or tp_price <= 0:
        return {"placed": False, "reason": "bad_qty_or_price"}
    coid = FROZEN_TP_PREFIX + _now().strftime("%Y%m%d%H%M")
    try:
        r = post_futures_order(
            symbol=sym, side="BUY", quantity=qty, price=tp_price, api_key=k, api_secret=s,
            time_in_force="GTC", position_side="SHORT", new_client_order_id=coid,
        )
        return {"placed": True, "qty": qty, "price": tp_price, "wavg": round(wavg, 5),
                "min_entry": min_entry, "orderId": r.get("orderId"), "coid": coid}
    except Exception as e:  # noqa: BLE001 - report the exchange error, keep going
        return {"placed": False, "reason": str(e)[:140]}


def cancel_frozen_tp(sym: str, k: str, s: str) -> int:
    n = 0
    try:
        for o in fetch_futures_open_orders(sym, k, s):
            if str(o.get("clientOrderId", "")).startswith(FROZEN_TP_PREFIX):
                try:
                    delete_futures_order(symbol=sym, order_id=int(o.get("orderId")), api_key=k, api_secret=s)
                    n += 1
                except Exception:
                    pass
    except Exception:
        pass
    return n


def _read_frozen_from_plan(workdir: str, slug: str, plan_json: str) -> tuple[float, float]:
    """frozen (long, short) qty to keep, preferring the plan, then the state file."""
    try:
        p = json.load(open(os.path.join(workdir, plan_json)))
        rf = ((p.get("best_quote_maker_volume") or {}).get("reduce_freeze") or {})
        return float(rf.get("frozen_long_qty") or 0.0), float(rf.get("frozen_short_qty") or 0.0)
    except Exception:
        pass
    try:
        st = json.load(open(os.path.join(workdir, "output", f"{slug}_loop_state.json")))
        fr = st.get("best_quote_frozen_inventory") or {}
        return (
            float(fr.get("long_qty") or fr.get("frozen_long_qty") or 0.0),
            float(fr.get("short_qty") or fr.get("frozen_short_qty") or 0.0),
        )
    except Exception:
        return 0.0, 0.0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--service", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--target", type=float, default=0.0,
                    help="Fallback target; overridden by the live control max_cumulative_notional.")
    ap.add_argument("--wear-stop", type=float, default=2.0)
    ap.add_argument("--first", type=float, default=75000.0)
    ap.add_argument("--plan-json", default=None,
                    help="Defaults to output/<symbol>_loop_latest_plan.json.")
    ap.add_argument("--tp-ratio", type=float, default=0.05)
    ap.add_argument("--tick", type=float, default=0.0001)
    ap.add_argument("--place-tp-now", action="store_true",
                    help="Rest the frozen-short TP for an already-stopped runner, then exit.")
    ap.add_argument("--enforce", action="store_true")
    return ap


def main() -> None:
    a = build_parser().parse_args()
    sym = a.symbol.upper()
    slug = sym.lower()
    plan_json = a.plan_json or f"output/{slug}_loop_latest_plan.json"
    k = os.environ["BINANCE_API_KEY"]
    s = os.environ["BINANCE_API_SECRET"]

    # Target tracks the runner max_cumulative_notional (daily_reset updates it each day),
    # so today/tomorrow target changes flow from the config without editing this cron.
    try:
        cfg = json.load(open(os.path.join(a.workdir, "output", f"{slug}_loop_runner_control.json")))
        ct = float(cfg.get("max_cumulative_notional") or 0)
        if ct > 0:
            a.target = ct
    except Exception:
        pass

    ts = _now().isoformat()

    # Manual: rest the frozen-short TP for an already-stopped runner, then exit.
    if a.place_tp_now:
        info = place_frozen_short_tp(sym, k, s, a.workdir, slug, a.tp_ratio, a.tick) if a.enforce else {"dry_run": True}
        print(json.dumps({"ts": ts, "place_tp_now": info}))
        return

    # Once the runner is back up it manages frozen itself -> clean up any stale resting TP.
    try:
        alive = subprocess.run(
            ["systemctl", "is-active", a.service], capture_output=True, text=True
        ).stdout.strip() == "active"
        if alive:
            c = cancel_frozen_tp(sym, k, s)
            if c:
                print(json.dumps({"ts": ts, "frozen_tp_canceled": c}))
    except Exception:
        pass

    done_marker = os.path.join(
        a.workdir, "output", f"{slug}_target_gate_done_{_now().strftime('%Y%m%d')}.flag"
    )
    if os.path.exists(done_marker):
        print(json.dumps({"ts": ts, "skip": "already_done_today"}))
        return

    vol, wear = daily_vol_wear(sym, k, s)
    hit_target = vol >= a.target
    hit_wear = vol >= a.first and wear > a.wear_stop
    status: dict[str, Any] = {
        "ts": ts, "vol": round(vol, 0), "wear": round(wear, 2), "target": a.target,
        "hit_target": hit_target, "hit_wear": hit_wear, "enforce": a.enforce,
    }
    if not (hit_target or hit_wear):
        print(json.dumps(status))
        return
    status["trigger"] = "target" if hit_target else "wear"
    if not a.enforce:
        status["action"] = "DRY_RUN_would_stop_flatten"
        print(json.dumps(status))
        return

    # 1. stop runner
    subprocess.run(["sudo", "-n", "systemctl", "stop", a.service], capture_output=True)
    # 2. read frozen inventory to keep; only flatten managed/non-frozen position.
    frozen_long, frozen_short = _read_frozen_from_plan(a.workdir, slug, plan_json)
    # 3. cancel open orders
    oo = fetch_futures_open_orders(sym, k, s)
    for o in oo:
        try:
            delete_futures_order(symbol=sym, order_id=int(o.get("orderId")), api_key=k, api_secret=s)
        except Exception:
            pass
    # 4. flatten managed only; preserve frozen long/short inventory.
    pos = fetch_futures_position_risk_v3(symbol=sym, api_key=k, api_secret=s, contract_type="usdm")
    d = {x.get("positionSide"): float(x.get("positionAmt", 0)) for x in pos}
    lo = d.get("LONG", 0.0)
    sh = -d.get("SHORT", 0.0)  # sh = short size positive
    close_long = int(max(lo - frozen_long, 0))
    close_short = int(max(sh - frozen_short, 0))
    acted: list[str] = []
    if close_long > 0:
        post_futures_market_order(symbol=sym, side="SELL", quantity=close_long, api_key=k, api_secret=s,
                                  position_side="LONG", new_client_order_id="tgtflatL")
        acted.append(f"closeLONG{close_long}")
    if close_short > 0:
        post_futures_market_order(symbol=sym, side="BUY", quantity=close_short, api_key=k, api_secret=s,
                                  position_side="SHORT", new_client_order_id="tgtflatS")
        acted.append(f"closeSHORT{close_short}")
    # 5. rest a take-profit LIMIT on the kept frozen shorts (fills automatically if price reaches
    #    the tp-ratio line while the runner is stopped; the runner cancels it on its next active cycle).
    tp = (
        place_frozen_short_tp(sym, k, s, a.workdir, slug, a.tp_ratio, a.tick)
        if frozen_short > 0 else {"placed": False, "reason": "no_frozen_short"}
    )
    Path(done_marker).write_text(ts, encoding="utf-8")
    status.update({
        "action": "STOPPED_FLATTENED_MANAGED_ONLY",
        "frozen_long_kept": frozen_long, "frozen_short_kept": frozen_short,
        "closed": acted, "canceled_orders": len(oo), "frozen_tp": tp,
    })
    print(json.dumps(status))


if __name__ == "__main__":
    main()
