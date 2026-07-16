#!/usr/bin/env python3
"""Symbol-generic server-side health monitor for hedge best-quote volume farming.

Tracked, deploy-via-git form of ``output/ops/reusdt_health_monitor.py``. It is the
"稳健" (stable) automation model: it NEVER boosts budget to chase a target the way a
pace controller does; it only keeps the runner alive and brakes wear. Complements
:mod:`grid_optimizer.competition_target_gate` (target / wear -> lifecycle intent) and the
daily reset cron (08:00 restart). Intended cron cadence: ``*/10``. Run as::

    python -m grid_optimizer.competition_health_monitor --symbol ARXUSDT \
        --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --enforce

Jobs:

1. Liveness watchdog: service active AND looping (``mid=`` in logs) AND 0 orders
   placed in ``--watchdog-min`` AND not in an intended-stop state -> reset-failed +
   restart. Catches the "active but not placing" deadlock class. Rate-limited.
2. Deadlock self-heal: looping but placed=0 for ``--deadlock-min`` and NOT a real
   terminal stop -> break the balanced-hedge deadlock by closing only the MATCHED
   managed qty ``min(managed_long, managed_short)`` (net-neutral, ~$0 realized; a
   one-sided legit loss-reduce has matched=0 and is left untouched). A spread hedge
   that would realize a loss on close is skipped. Frozen inventory is preserved.
3. Wear governor (保量 / 限速压磨损 / 绝不停): after ``--first`` and BELOW
   ``--hard-wear`` (the gate's hard stop), if recent wear stays above ``--brake-wear``
   for two consecutive runs AND the day is also creeping up -> raise
   ``quote_offset_ticks`` by 1 (brake, up to ``--max-offset``) + restart; if recent
   wear recovers below ``--release-wear`` -> lower it toward ``--min-offset``.
   Hysteretic + rate-limited. Slows the fill/wear rate without ever stopping farming.
4. Health log: one JSON line per run to ``output/<symbol>_health_monitor.log``.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from .recovery_control_ownership import is_recovery_managed

# A matched-hedge close is only allowed to auto-fire when it realizes no worse than
# this small amount (a near net-neutral hedge). A spread hedge (long-high/short-low)
# would realize the spread as a loss, so the deadlock self-heal leaves it for the user.
MAX_MATCHED_LOSS = 3.0

OFFSET_KEYS = ("best_quote_maker_volume_quote_offset_ticks", "quote_offset_ticks")

# Journal markers of a TERMINAL / intended stop (runtime guard, end window, target
# cap, loss recovery). Any restart-capable path must not touch a runner in one of
# these states: the stop encodes a risk decision.
TERMINAL_STOP_MARKERS = ("stop_reason", "cooling_down", "after_end_window",
                         "max_cumulative", "runtime_guard_loss_recovery")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def is_active(service: str) -> bool:
    return subprocess.run(
        ["systemctl", "is-active", service], capture_output=True, text=True
    ).stdout.strip() == "active"


def journal(service: str, minutes: int) -> str:
    try:
        return subprocess.run(
            ["journalctl", "-u", service, "--since", f"{minutes} min ago", "--no-pager"],
            capture_output=True, text=True, timeout=40,
        ).stdout
    except Exception:
        return ""


def placed_sum(jout: str) -> int:
    return sum(int(m.group(1)) for m in re.finditer(r"placed=(\d+)", jout))


def daily_recent_wear(sym: str, k: str, s: str, recent_min: int) -> tuple[float, float, float, float]:
    """Return (day vol, day wear, recent vol, recent wear) from exchange userTrades."""
    from .data import fetch_futures_user_trades
    from .monitor import summarize_user_trades

    now = _now()
    start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    cur = int(start.timestamp() * 1000)
    trades: list[dict[str, Any]] = []
    seen: set[Any] = set()
    pages = 0
    while pages < 80:
        pages += 1
        b = fetch_futures_user_trades(symbol=sym, api_key=k, api_secret=s, start_time_ms=cur, limit=1000)
        new = [t for t in b if t.get("id") not in seen]
        for t in new:
            seen.add(t.get("id"))
        trades += new
        if len(b) < 1000 or not new:
            break
        cur = max(int(t.get("time", 0)) for t in b)
    su = summarize_user_trades(trades)
    v = su["gross_notional"]
    r = su["realized_pnl"]
    day_wear = (-r / v * 1e4) if v > 0 else 0.0
    rcut = int((now - timedelta(minutes=recent_min)).timestamp() * 1000)
    rt = [t for t in trades if int(t.get("time", 0)) >= rcut]
    rsu = summarize_user_trades(rt)
    rv = rsu["gross_notional"]
    rr = rsu["realized_pnl"]
    recent_wear = (-rr / rv * 1e4) if rv > 0 else 0.0
    return v, day_wear, rv, recent_wear


def get_offset(cfg_path: str) -> tuple[str, int]:
    c = json.load(open(cfg_path))
    for key in OFFSET_KEYS:
        if c.get(key) is not None:
            return key, int(c.get(key) or 0)
    return OFFSET_KEYS[0], 0


def apply_offset(cfg_path: str, key: str, val: int, updated_by: str) -> None:
    """Atomically patch one key in the runner control JSON.

    Self-contained equivalent of the server ``/tmp/apply_cfg.py`` helper: it keeps a
    timestamped backup and stamps ``updated_at`` / ``updated_by`` so a governor change
    is auditable. The runner only reads control at startup, so the caller restarts.
    """
    d = json.load(open(cfg_path))
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    bak = f"{cfg_path}.bak_{updated_by}_{stamp}"
    try:
        with open(bak, "w") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    d[key] = int(val)
    d["updated_at"] = _now().astimezone().isoformat()
    d["updated_by"] = updated_by
    tmp = cfg_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
    os.replace(tmp, cfg_path)


def restart(service: str) -> dict[str, Any]:
    """Restart the service and report the systemctl return codes.

    The caller must NOT assume the runner restarted: it inspects ``ok`` and only
    advances its rate-limit / did_restart bookkeeping when the restart actually
    succeeded, so a failed restart is retried instead of silently swallowed.
    """
    reset = subprocess.run(["sudo", "-n", "systemctl", "reset-failed", service], capture_output=True)
    res = subprocess.run(["sudo", "-n", "systemctl", "restart", service], capture_output=True, text=True)
    ok = res.returncode == 0
    out: dict[str, Any] = {"ok": ok, "reset_failed_rc": reset.returncode, "restart_rc": res.returncode}
    if not ok:
        out["error"] = (res.stderr or "").strip()[:160]
    return out


def deadlock_unstick(
    sym: str, k: str, s: str, workdir: str, slug: str, min_matched: int, auto_close: bool = False
) -> dict[str, Any]:
    """Detect the balanced-hedge deadlock; only close it when ``auto_close`` is set.

    When the runner holds a long AND a short both near the cap and a slightly
    underwater leg has frozen entries+exits, it can sit looping with placed=0. The
    ONLY net-neutral remedy is to close the MATCHED managed qty
    ``min(managed_long, managed_short)`` (non-zero only in a balanced hedge; a
    one-sided legit loss-reduce has matched=0). But auto market-reducing managed
    positions conflicts with the "no automatic managed pair-reduce" policy, so it is
    DISABLED by default: this function reports the detection (for alerting) and fires
    the matched close only when the operator explicitly opts in with ``auto_close``.
    Frozen inventory is always kept.
    """
    from .data import fetch_futures_position_risk_v3, post_futures_market_order

    pos = fetch_futures_position_risk_v3(symbol=sym, api_key=k, api_secret=s, contract_type="usdm")
    d = {x.get("positionSide"): float(x.get("positionAmt", 0) or 0) for x in pos}
    e = {x.get("positionSide"): float(x.get("entryPrice", 0) or 0) for x in pos}
    lo = max(d.get("LONG", 0.0), 0.0)
    sh = max(-d.get("SHORT", 0.0), 0.0)
    long_entry = max(e.get("LONG", 0.0), 0.0)
    short_entry = max(e.get("SHORT", 0.0), 0.0)
    fl = fs = 0.0
    try:
        p = json.load(open(os.path.join(workdir, "output", f"{slug}_loop_latest_plan.json")))
        rf = ((p.get("best_quote_maker_volume") or {}).get("reduce_freeze") or {})
        fl = float(rf.get("frozen_long_qty") or 0.0)
        fs = float(rf.get("frozen_short_qty") or 0.0)
    except Exception:
        pass
    mlong = max(lo - fl, 0.0)
    mshort = max(sh - fs, 0.0)
    matched = int(min(mlong, mshort))
    # Closing the matched hedge realizes matched*(short_entry - long_entry): ~$0 only when
    # the two legs were opened at the same price (a true net-neutral hedge). A spread hedge
    # (long high, short low) would realize the spread as a LOSS -- skip it, per "never flatten
    # at a loss".
    est_close_pnl = matched * (short_entry - long_entry) if (long_entry > 0 and short_entry > 0) else 0.0
    out: dict[str, Any] = {"managed_long": round(mlong, 1), "managed_short": round(mshort, 1),
                           "matched": matched, "est_close_pnl": round(est_close_pnl, 2)}
    if matched < int(min_matched):
        out["acted"] = False
        out["reason"] = "no_matched_hedge"
        return out
    if est_close_pnl < -MAX_MATCHED_LOSS:
        out["acted"] = False
        out["reason"] = "spread_hedge_would_realize_loss"
        return out
    # A closable net-neutral hedge was found, but auto managed pair-reduce is off by policy:
    # report it for alerting and place NO market order unless the operator opted in.
    if not auto_close:
        out["acted"] = False
        out["reason"] = "blocked_by_config"
        out["would_unstick"] = True
        return out
    out["acted"] = True
    try:
        post_futures_market_order(symbol=sym, side="SELL", quantity=matched, api_key=k, api_secret=s,
                                  position_side="LONG", new_client_order_id="deadlockL")
        post_futures_market_order(symbol=sym, side="BUY", quantity=matched, api_key=k, api_secret=s,
                                  position_side="SHORT", new_client_order_id="deadlockS")
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:120]
    return out


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--service", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--first", type=float, default=75000.0)
    ap.add_argument("--hard-wear", type=float, default=2.0)
    ap.add_argument("--brake-wear", type=float, default=3.0)       # recent wear that arms a brake
    ap.add_argument("--brake-day-wear", type=float, default=1.5)   # only brake if the DAY is also creeping up
    ap.add_argument("--release-wear", type=float, default=1.0)     # recent wear that releases a brake
    ap.add_argument("--max-offset", type=int, default=3)
    ap.add_argument("--min-offset", type=int, default=0)           # governor never releases below this base offset
    ap.add_argument("--watchdog-min", type=int, default=6)
    ap.add_argument("--deadlock-min", type=int, default=16)            # placed=0 this long while looping = deadlock
    ap.add_argument("--deadlock-cooldown-min", type=int, default=25)   # min gap between auto-unsticks
    ap.add_argument("--min-matched-qty", type=int, default=20)         # only unstick a hedge at least this big
    ap.add_argument("--recent-min", type=int, default=20)
    ap.add_argument("--governor-cooldown-min", type=int, default=20)
    ap.add_argument("--enable-deadlock-unstick", action="store_true",
                    help="Opt in to the automatic net-neutral matched managed close. OFF by "
                         "default (policy: no automatic managed pair-reduce); when off the monitor "
                         "only detects and logs the deadlock. Production cron should leave this off.")
    ap.add_argument("--enforce", action="store_true")
    return ap


def main() -> None:
    a = build_parser().parse_args()
    sym = a.symbol.upper()
    slug = sym.lower()
    wd = a.workdir
    cfg = os.path.join(wd, "output", f"{slug}_loop_runner_control.json")
    state_path = os.path.join(wd, "output", f"{slug}_health_monitor_state.json")
    log_path = os.path.join(wd, "output", f"{slug}_health_monitor.log")
    try:
        st = json.load(open(state_path))
    except Exception:
        st = {}
    tnow = time.time()
    rec: dict[str, Any] = {"ts": _now().isoformat(), "symbol": sym, "enforce": a.enforce}
    did_restart = False

    try:
        control = json.load(open(cfg, encoding="utf-8"))
    except Exception:
        control = {}
    recovery_managed = is_recovery_managed(sym, control if isinstance(control, dict) else {})
    enforce_actions = a.enforce and not recovery_managed

    active = is_active(a.service)
    jout = journal(a.service, a.watchdog_min)
    placed = placed_sum(jout)
    looping = "mid=" in jout
    # "intended" = the runner is deliberately not entering (a real stop OR any logged pause /
    # loss-reduce / cooling state), so the watchdog must NOT mistake it for a stuck-not-placing
    # deadlock. Only a runner that is looping but logs none of these is treated as hung.
    terminal_stop = any(x in jout for x in TERMINAL_STOP_MARKERS)
    intended = terminal_stop or any(x in jout for x in ("net_loss_reduce", "pause:", "buy_paused=yes",
                                                        "reduce_only", "inventory_recover"))
    rec.update({"active": active, "looping": looping, f"placed_{a.watchdog_min}m": placed,
                "intended_stop": intended, "terminal_stop": terminal_stop})
    if recovery_managed:
        rec["action"] = "observe_only_recovery_managed_symbol"

    # --- 1. liveness watchdog (only when alive+looping but not placing; rate-limited) ---
    last_wd = float(st.get("last_watchdog_ts") or 0)
    if active and looping and placed == 0 and not intended and (tnow - last_wd) >= a.watchdog_min * 60:
        rec["watchdog"] = "restart_not_placing"
        if enforce_actions:
            rr = restart(a.service)
            rec["watchdog_restart"] = rr
            if rr["ok"]:                      # a failed restart is retried next cycle, not swallowed
                st["last_watchdog_ts"] = tnow
                did_restart = True

    # --- 1b. deadlock self-heal: looping but placed=0 for --deadlock-min and NOT a real terminal
    #     stop (the runner's own pauses like buy_paused/net_loss_reduce ARE the deadlock symptom,
    #     so they are NOT excluded here -- only true stops are). A restart would just re-stall, so
    #     instead break the balanced hedge with a net-neutral matched close. Rate-limited. ---
    deadlock: dict[str, Any] = {}
    try:
        if not did_restart and active and looping:
            dj = journal(a.service, a.deadlock_min)
            placed_dl = placed_sum(dj)
            terminal = any(x in dj for x in TERMINAL_STOP_MARKERS)
            deadlock = {f"placed_{a.deadlock_min}m": placed_dl, "terminal": terminal}
            last_dl = float(st.get("last_deadlock_ts") or 0)
            if placed_dl == 0 and not terminal and (tnow - last_dl) >= a.deadlock_cooldown_min * 60:
                if enforce_actions:
                    kk = os.environ["BINANCE_API_KEY"]
                    ss = os.environ["BINANCE_API_SECRET"]
                    deadlock["unstick"] = deadlock_unstick(
                        sym, kk, ss, wd, slug, a.min_matched_qty,
                        auto_close=a.enable_deadlock_unstick,
                    )
                    if (deadlock["unstick"] or {}).get("acted"):
                        st["last_deadlock_ts"] = tnow
                        did_restart = True   # skip the governor this cycle after acting
                else:
                    deadlock["unstick"] = {"dry_run": True}
    except Exception as e:  # noqa: BLE001
        deadlock = {"error": str(e)[:100]}
    rec["deadlock"] = deadlock

    # --- 2. wear governor (保量/限速压磨损/绝不停) ---
    gov: dict[str, Any] = {}
    try:
        k = os.environ["BINANCE_API_KEY"]
        s = os.environ["BINANCE_API_SECRET"]
        vol, day_wear, rvol, rwear = daily_recent_wear(sym, k, s, a.recent_min)
        okey, off = get_offset(cfg)
        gov = {"vol": round(vol, 0), "day_wear": round(day_wear, 2), "rwear": round(rwear, 2), "offset": off}
        last_gov = float(st.get("last_governor_ts") or 0)
        gov_cooled = (tnow - last_gov) >= a.governor_cooldown_min * 60
        streak = int(st.get("high_streak") or 0)
        # Brake only when the recent window is hot AND the day metric is genuinely creeping up
        # toward the hard stop -- so a noisy 20m spike on an otherwise healthy day does not
        # throttle volume (保量 first). Release when recent wear recovers OR the day cools off.
        # Gate on `active` AND `not terminal_stop`: the brake/release apply a config change
        # with a restart, which must never touch a runner the gate/guard intentionally
        # stopped -- even one still process-alive holding in a runtime-guard stopped loop.
        if not did_restart and active and not terminal_stop and vol >= a.first and day_wear < a.hard_wear:
            if rwear > a.brake_wear and day_wear > a.brake_day_wear:
                streak += 1
                if streak >= 2 and off < a.max_offset and gov_cooled:
                    gov["action"] = "brake_%d->%d" % (off, off + 1)
                    if enforce_actions:
                        apply_offset(cfg, okey, off + 1, "health_governor_brake")
                        rr = restart(a.service)
                        gov["restart"] = rr
                        if rr["ok"]:
                            st["last_governor_ts"] = tnow
                            did_restart = True
                    streak = 0
            elif off > a.min_offset and (rwear < a.release_wear or day_wear < a.brake_day_wear - 0.2) and gov_cooled:
                gov["action"] = "release_%d->%d" % (off, off - 1)
                if enforce_actions:
                    apply_offset(cfg, okey, off - 1, "health_governor_release")
                    rr = restart(a.service)
                    gov["restart"] = rr
                    if rr["ok"]:
                        st["last_governor_ts"] = tnow
                        did_restart = True
                streak = 0
            else:
                streak = 0
        else:
            streak = 0
        st["high_streak"] = streak
    except Exception as e:  # noqa: BLE001
        gov["error"] = str(e)[:100]
    rec["governor"] = gov

    try:
        json.dump(st, open(state_path, "w"))
    except Exception:
        pass
    line = json.dumps(rec)
    try:
        with open(log_path, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line)


if __name__ == "__main__":
    main()
