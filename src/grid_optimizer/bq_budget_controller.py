"""Budget-only pace controller v2 (flow/reservoir aware).

Successor of the ops script ``output/ops/bq_budget_controller.py``. Tiers pick
ONLY the cycle budget; offset is owned by competition_health_monitor and the
runner is never stopped here.

Tier semantics (same contract as v1, ``--budgets`` order is
``defensive,cautious,cruise,boost1,boost2``):

  defensive : flow wear/pnl bleeding, or reservoir day budget breached
  cautious  : mild flow bleeding
  cruise    : on pace (also used for tier "done")
  boost1    : behind pace (<95% required rate) and healthy
  boost2    : far behind (<70%) or 30-minute stall, and healthy

v2 changes vs the ops v1:

* Health checks use the FLOW line only: trades with ``|realizedPnl| <= cut``
  plus their commission (converted to USDT per ``commissionAsset``). Large
  realizations (reservoir releases / manual pulses) no longer brake the flow.
* The reservoir line gets its own protection instead: day sum of large
  realizations (net of their commission) plus funding fees must stay above
  ``-reservoir_day_budget`` or the tier is forced to defensive and a flag
  file is written for human review.
* Fail-safe direction is unified: missing/stale ``latest_plan``, income API
  failure or commission conversion failure all BLOCK boost tiers (they never
  silently allow them).
* Self-check: boost budgets are clamped to the runner's
  ``hedge_bq_position_drift_tolerance_notional``; if that tolerance is
  missing/zero, boost is banned entirely.

Runs both as a module (``python -m grid_optimizer.bq_budget_controller``) and
as a plain script file (v1 ops form, e.g. copied to ``output/ops/``): data
imports go through :func:`_import_data`, which falls back to a sys.path
bootstrap when no package context exists.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

FLOW_PNL_CUT_DEFAULT = 0.3
PLAN_MAX_AGE_SECONDS_DEFAULT = 300.0
VOL_DEFENSIVE_REASONS = {"high_volatility_defensive", "extreme_volatility_defensive"}
STABLE_FEE_ASSETS = {"USDT", "USDC", "FDUSD", "BUSD"}

TIER_DEFENSIVE = "defensive"
TIER_CAUTIOUS = "cautious"
TIER_CRUISE = "cruise"
TIER_BOOST1 = "boost1"
TIER_BOOST2 = "boost2"
TIER_DONE = "done"

_CYCLE_BUDGET_KEY = "best_quote_maker_volume_cycle_budget_notional"
_MIN_CYCLE_BUDGET_KEY = "best_quote_maker_volume_min_cycle_budget_notional"
_TIER_BUDGETS_KEY = "budget_controller_tier_budgets"


@dataclass(frozen=True)
class Budgets:
    defensive: float
    cautious: float
    cruise: float
    boost1: float
    boost2: float


@dataclass
class FlowSplit:
    flow_pnl: float = 0.0
    flow_vol: float = 0.0
    flow_trades: int = 0
    reservoir_realized: float = 0.0
    reservoir_trades: int = 0
    fee_stale: bool = False

    @property
    def flow_wear(self) -> float:
        if self.flow_vol <= 0:
            return 0.0
        return -self.flow_pnl / self.flow_vol * 1e4


def parse_budgets(raw: object) -> Budgets:
    if isinstance(raw, Budgets):
        return raw
    if isinstance(raw, str):
        parts: Sequence[object] = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, Sequence):
        parts = raw
    else:
        raise ValueError("budget tiers must be a comma-separated string or a five-item list")
    if len(parts) != 5:
        raise ValueError(
            "budget tiers must have exactly 5 values: defensive,cautious,cruise,boost1,boost2"
        )
    values = [float(p) for p in parts]
    if any(v <= 0 for v in values):
        raise ValueError("--budgets values must all be > 0")
    return Budgets(*values)


def resolve_budget_tiers(
    control: dict[str, Any], override: object | None = None
) -> tuple[Budgets, str]:
    if override is not None:
        return parse_budgets(override), "cli_override"
    configured_tiers = control.get(_TIER_BUDGETS_KEY)
    if configured_tiers is not None:
        return parse_budgets(configured_tiers), "control_tiers"
    configured_cycle = float(control.get(_CYCLE_BUDGET_KEY) or 0.0)
    if configured_cycle <= 0:
        raise ValueError(f"missing positive {_CYCLE_BUDGET_KEY} in runner control")
    return Budgets(*([configured_cycle] * 5)), "runner_cycle_budget"


def resolve_min_cycle(control: dict[str, Any], override: float | None = None) -> float:
    value = override if override is not None else control.get(_MIN_CYCLE_BUDGET_KEY)
    resolved = float(value or 0.0)
    if resolved <= 0:
        raise ValueError(f"missing positive {_MIN_CYCLE_BUDGET_KEY} in runner control")
    return resolved


def convert_commission_usdt(
    commission: float, asset: str | None, bnb_price: float | None
) -> tuple[float, bool]:
    """Return (commission_in_usdt, ok). Never guesses on unknown assets."""
    if not commission:
        return 0.0, True
    asset_norm = (asset or "").upper().strip()
    if asset_norm in STABLE_FEE_ASSETS:
        return float(commission), True
    if asset_norm == "BNB":
        if bnb_price is not None and bnb_price > 0:
            return float(commission) * float(bnb_price), True
        return 0.0, False
    return 0.0, False


def split_flow_reservoir(
    trades: Iterable[dict[str, Any]],
    *,
    cut: float = FLOW_PNL_CUT_DEFAULT,
    bnb_price: float | None = None,
) -> FlowSplit:
    out = FlowSplit()
    for t in trades:
        try:
            rpnl = float(t.get("realizedPnl") or 0.0)
            comm = float(t.get("commission") or 0.0)
            qq = abs(float(t.get("quoteQty") or 0.0))
        except (TypeError, ValueError):
            continue
        comm_usdt, ok = convert_commission_usdt(comm, t.get("commissionAsset"), bnb_price)
        if not ok:
            out.fee_stale = True
        if abs(rpnl) <= cut:
            out.flow_pnl += rpnl - comm_usdt
            out.flow_vol += qq
            out.flow_trades += 1
        else:
            out.reservoir_realized += rpnl - comm_usdt
            out.reservoir_trades += 1
    return out


def assess_vol_defensive(
    plan_path: str, *, now_s: float, max_age_s: float = PLAN_MAX_AGE_SECONDS_DEFAULT
) -> tuple[bool, bool]:
    """Return (vol_defensive, plan_stale). Any anomaly -> (True, True): a
    missing/stale/broken plan must block boost, never silently allow it."""
    try:
        mtime = os.path.getmtime(plan_path)
    except OSError:
        return True, True
    if now_s - mtime > max_age_s:
        return True, True
    try:
        with open(plan_path, encoding="utf-8") as f:
            plan = json.load(f)
    except (OSError, ValueError):
        return True, True
    bq = plan.get("best_quote_maker_volume")
    metrics = bq.get("metrics") if isinstance(bq, dict) else None
    dc = metrics.get("dynamic_control") if isinstance(metrics, dict) else None
    if not isinstance(dc, dict) or "reason" not in dc:
        return True, True
    return str(dc.get("reason") or "") in VOL_DEFENSIVE_REASONS, False


def clamp_boost_budgets(
    budgets: Budgets, drift_tolerance: float | None
) -> tuple[Budgets, bool, bool]:
    """Return (budgets, boost_clamped, boost_banned).

    A boost tier may place up to its budget notional in one cycle; the
    runner's submit-time position drift tolerance must cover that, so boost
    budgets are clamped to the tolerance. Unknown/zero tolerance bans boost.
    """
    if drift_tolerance is None or drift_tolerance <= 0:
        return budgets, False, True
    b1 = min(budgets.boost1, drift_tolerance)
    b2 = min(budgets.boost2, drift_tolerance)
    clamped = (b1 != budgets.boost1) or (b2 != budgets.boost2)
    if clamped:
        budgets = replace(budgets, boost1=b1, boost2=b2)
    return budgets, clamped, False


def select_tier(
    *,
    remaining: float,
    required_rate: float,
    vol_1h: float,
    vol_30m: float,
    flow: FlowSplit,
    budgets: Budgets,
    vol_defensive: bool,
    plan_stale: bool,
    income_stale: bool,
    fee_stale: bool,
    reservoir_day: float,
    reservoir_day_budget: float,
    boost_banned: bool,
) -> tuple[str, float, list[str]]:
    reasons: list[str] = []
    reservoir_breach = reservoir_day_budget > 0 and reservoir_day <= -reservoir_day_budget
    if reservoir_breach:
        reasons.append("reservoir_budget_breach")
    for name, stale in (
        ("plan_stale", plan_stale),
        ("income_stale", income_stale),
        ("fee_stale", fee_stale),
    ):
        if stale:
            reasons.append(name)
    if boost_banned:
        reasons.append("boost_banned_no_drift_tolerance")

    # breach overrides "done": even with the day target met, a breached
    # reservoir budget must pin the entry budget at defensive, not cruise
    if reservoir_breach:
        return TIER_DEFENSIVE, budgets.defensive, reasons

    if remaining <= 0:
        return TIER_DONE, budgets.cruise, reasons

    wear = flow.flow_wear
    pnl = flow.flow_pnl
    if (flow.flow_vol >= 1500 and wear > 3.0) or pnl <= -4.0:
        return TIER_DEFENSIVE, budgets.defensive, reasons
    if (flow.flow_vol >= 1500 and wear > 1.5) or pnl <= -2.0:
        return TIER_CAUTIOUS, budgets.cautious, reasons

    wear_ok = wear <= 3.0 and pnl > -2.0
    boost_allowed = (
        wear_ok
        and not vol_defensive
        and not plan_stale
        and not income_stale
        and not fee_stale
        and not boost_banned
    )
    if boost_allowed and (vol_1h < 0.70 * required_rate or vol_30m < 500):
        return TIER_BOOST2, budgets.boost2, reasons
    if boost_allowed and vol_1h < 0.95 * required_rate:
        return TIER_BOOST1, budgets.boost1, reasons
    return TIER_CRUISE, budgets.cruise, reasons


def reservoir_flag_path(workdir: str, symbol: str) -> str:
    return os.path.join(workdir, "output", f"{symbol.upper()}_RESERVOIR_BUDGET.flag")


def maybe_write_reservoir_flag(
    path: str, status: dict[str, Any], *, breach: bool, enforce: bool
) -> bool:
    if not (breach and enforce):
        return False
    with open(path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=1, default=str)
    return True


def _import_data():
    """Import grid_optimizer.data in both run forms (module or plain script)."""
    try:
        from . import data  # type: ignore[attr-defined]
        return data
    except ImportError:
        here = os.path.dirname(os.path.abspath(__file__))
        for candidate in (os.path.dirname(here), os.path.join(os.getcwd(), "src")):
            if candidate not in sys.path and os.path.isdir(candidate):
                sys.path.insert(0, candidate)
        import grid_optimizer.data as data
        return data


def _fetch_day_trades(symbol: str, api_key: str, api_secret: str, day_start_ms: int):
    # NOTE: keeps v1 ops pagination verbatim (cursor on max trade time, page
    # cap 90, no explicit end_time_ms) for behavioural parity with the
    # deployed controller; switching to data.fetch_time_paged() is a separate
    # cleanup once v2 has soaked.
    fetch_futures_user_trades = _import_data().fetch_futures_user_trades

    cur = day_start_ms
    trades: list[dict[str, Any]] = []
    seen: set[Any] = set()
    pages = 0
    while pages < 90:
        pages += 1
        batch = fetch_futures_user_trades(
            symbol=symbol, api_key=api_key, api_secret=api_secret,
            start_time_ms=cur, limit=1000,
        )
        new = [t for t in batch if t.get("id") not in seen]
        for t in new:
            seen.add(t.get("id"))
        trades += new
        if len(batch) < 1000 or not new:
            break
        cur = max(int(t.get("time", 0)) for t in batch)
    return trades


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--runner-wrapper", required=True)
    ap.add_argument(
        "--budgets",
        default=None,
        help="optional override; default reads budget_controller_tier_budgets or runner cycle",
    )
    ap.add_argument("--min-cycle", type=float, default=None)
    ap.add_argument("--flow-pnl-cut", type=float, default=FLOW_PNL_CUT_DEFAULT,
                    help="|realizedPnl| <= cut counts as flow, else reservoir")
    ap.add_argument("--reservoir-day-budget", type=float, default=0.0,
                    help="force defensive when day reservoir loss exceeds this (0=off)")
    ap.add_argument("--plan-max-age-seconds", type=float,
                    default=PLAN_MAX_AGE_SECONDS_DEFAULT)
    ap.add_argument("--enforce", action="store_true")
    a = ap.parse_args(argv)

    api_key = os.environ["BINANCE_API_KEY"]
    api_secret = os.environ["BINANCE_API_SECRET"]
    now = datetime.now(timezone.utc)
    prefix = a.symbol.lower()
    control_path = os.path.join(a.workdir, "output", f"{prefix}_loop_runner_control.json")
    plan_path = os.path.join(a.workdir, "output", f"{prefix}_loop_latest_plan.json")
    with open(control_path, encoding="utf-8") as f:
        control = json.load(f)
    budgets_raw, budget_source = resolve_budget_tiers(control, a.budgets)
    min_cycle = resolve_min_cycle(control, a.min_cycle)

    day_start_ms = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp() * 1000)
    trades = _fetch_day_trades(a.symbol, api_key, api_secret, day_start_ms)

    now_ms = int(now.timestamp() * 1000)
    h_ms = now_ms - 3_600_000
    m30_ms = now_ms - 1_800_000
    trades_1h = [t for t in trades if int(t.get("time", 0)) >= h_ms]
    vol_day = sum(abs(float(t.get("quoteQty") or 0)) for t in trades)
    vol_1h = sum(abs(float(t.get("quoteQty") or 0)) for t in trades_1h)
    vol_30m = sum(abs(float(t.get("quoteQty") or 0)) for t in trades
                  if int(t.get("time", 0)) >= m30_ms)

    bnb_price: float | None = None
    if any((t.get("commissionAsset") or "").upper() == "BNB" for t in trades):
        try:
            bnb_price = _import_data().fetch_futures_latest_price("BNBUSDT")
        except Exception:
            bnb_price = None

    flow_1h = split_flow_reservoir(trades_1h, cut=a.flow_pnl_cut, bnb_price=bnb_price)
    day_split = split_flow_reservoir(trades, cut=a.flow_pnl_cut, bnb_price=bnb_price)
    # any conversion failure anywhere today blocks boost, not just in the 1h window
    fee_stale = flow_1h.fee_stale or day_split.fee_stale

    funding_day = 0.0
    income_stale = False
    try:
        rows = _import_data().fetch_futures_income_history(
            api_key=api_key, api_secret=api_secret, symbol=a.symbol,
            income_type="FUNDING_FEE", start_time_ms=day_start_ms,
        )
        funding_day = sum(float(r.get("income") or 0.0) for r in rows)
    except Exception:
        income_stale = True
    reservoir_day = day_split.reservoir_realized + funding_day

    target = float(control.get("max_cumulative_notional") or 0)
    day_end_s = day_start_ms / 1000 + 86400
    hours_left = max((day_end_s - now.timestamp()) / 3600.0, 0.25)
    remaining = max(target - vol_day, 0.0)
    required_rate = remaining / hours_left

    vol_defensive, plan_stale = assess_vol_defensive(
        plan_path, now_s=now.timestamp(), max_age_s=a.plan_max_age_seconds
    )

    tolerance_raw = control.get("hedge_bq_position_drift_tolerance_notional")
    try:
        drift_tolerance = float(tolerance_raw) if tolerance_raw is not None else None
    except (TypeError, ValueError):
        drift_tolerance = None
    budgets, boost_clamped, boost_banned = clamp_boost_budgets(budgets_raw, drift_tolerance)

    tier, budget, reasons = select_tier(
        remaining=remaining, required_rate=required_rate, vol_1h=vol_1h,
        vol_30m=vol_30m, flow=flow_1h, budgets=budgets,
        vol_defensive=vol_defensive, plan_stale=plan_stale,
        income_stale=income_stale, fee_stale=fee_stale, reservoir_day=reservoir_day,
        reservoir_day_budget=a.reservoir_day_budget, boost_banned=boost_banned,
    )
    reservoir_breach = "reservoir_budget_breach" in reasons

    status = {
        "ts": now.isoformat(), "sym": a.symbol,
        "vol_day": round(vol_day), "target": target,
        "required_rate": round(required_rate),
        "vol_1h": round(vol_1h), "vol_30m": round(vol_30m),
        "flow_wear_1h": round(flow_1h.flow_wear, 2),
        "flow_pnl_1h": round(flow_1h.flow_pnl, 2),
        "flow_vol_1h": round(flow_1h.flow_vol),
        "reservoir_day": round(reservoir_day, 2),
        "reservoir_realized_day": round(day_split.reservoir_realized, 2),
        "funding_day": round(funding_day, 2),
        "reservoir_day_budget": a.reservoir_day_budget,
        "vol_defensive": vol_defensive, "plan_stale": plan_stale,
        "income_stale": income_stale, "fee_stale": fee_stale,
        "bnb_price": bnb_price, "drift_tolerance": drift_tolerance,
        "boost_clamped": boost_clamped, "boost_banned": boost_banned,
        "tier": tier, "budget": budget, "reasons": reasons,
        "budget_source": budget_source,
        "resolved_budgets": {
            "defensive": budgets.defensive,
            "cautious": budgets.cautious,
            "cruise": budgets.cruise,
            "boost1": budgets.boost1,
            "boost2": budgets.boost2,
        },
        "min_cycle": min_cycle,
    }
    current = float(control.get("best_quote_maker_volume_cycle_budget_notional", -1))
    changed = current != budget
    status["changed"] = changed
    flag_written = maybe_write_reservoir_flag(
        reservoir_flag_path(a.workdir, a.symbol), status,
        breach=reservoir_breach, enforce=a.enforce,
    )
    status["reservoir_flag_written"] = flag_written
    if a.enforce and changed:
        control["best_quote_maker_volume_cycle_budget_notional"] = budget
        control["best_quote_maker_volume_min_cycle_budget_notional"] = min_cycle
        control["budget_controller_tier"] = tier
        control["budget_controller_updated_at"] = status["ts"]
        temporary_path = f"{control_path}.tmp"
        with open(temporary_path, "w", encoding="utf-8") as f:
            json.dump(control, f, indent=1)
            f.write("\n")
        os.replace(temporary_path, control_path)
        completed = subprocess.run(
            [a.runner_wrapper, "restart", a.symbol],
            capture_output=True,
            text=True,
            check=False,
        )
        status["applied"] = True
        status["restart_returncode"] = completed.returncode
        if completed.returncode != 0:
            status["restart_stderr"] = completed.stderr[-1000:]
    print(json.dumps(status))
    return 0


if __name__ == "__main__":
    sys.exit(main())
