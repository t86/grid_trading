#!/usr/bin/env python3
"""Symbol-generic server-side target gate for hedge best-quote volume farming.

This is the tracked, deploy-via-git form of the hand-maintained
``output/ops/<symbol>_target_gate.py`` scripts (REUSDT / ARXUSDT / OUSDT were
byte-identical apart from the symbol and file paths). Run it as::

    python -m grid_optimizer.competition_target_gate --symbol ARXUSDT \
        --service grid-loop@ARXUSDT.service --workdir /home/ubuntu/wangge --enforce

Automatic behaviour:

1. Validate one immutable run contract before reading exchange state.
2. Read volume / wear from exchange ``userTrades`` only for that contract's
   half-open window (``-realized_pnl / gross_notional * 1e4`` = wear per 10k).
3. When the run-window volume hits ``max_cumulative_notional``, or the wear
   limit is breached, atomically persist a symbol-scoped ``lifecycle_drain``
   intent bound to the same contract digest.
4. Never stop/restart the runner, cancel exchange orders or submit close orders.
   The loop runner is the sole owner that consumes the intent and executes its
   maker-only terminal-drain state machine.

``--place-tp-now`` remains an explicit manual maintenance command.  It is not
part of the automatic target/wear enforcement path.

Legacy ``<symbol>_target_gate_done_YYYYMMDD.flag`` files are still observed for
backward compatibility.  New automatic runs use the idempotent terminal-intent
file instead and do not create a done marker.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shlex
import subprocess
import time
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
from .futures_run_lifecycle import (
    resolve_authoritative_run_contract,
    run_contract_identity_from_config,
    run_contract_snapshot_from_config,
    validate_run_contract,
)
from .futures_terminal_ownership import terminal_intent_id
from .futures_trade_window import fetch_exact_futures_trade_window
from .recovery_control_ownership import exclusive_control_lock

FROZEN_TP_PREFIX = "FROZENTP"
ACTIVE_TP_PREFIX = "TGTTP"
LIFECYCLE_INTENT_SCHEMA = "futures_lifecycle_intent_v2"
LIFECYCLE_INTENT_SOURCE = "competition_target_gate"
LIFECYCLE_INTENT_COMPLETED_STATUSES = frozenset(
    {"completed", "stopped_clean", "stopped_preserved"}
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _truncate_step(qty: float, step: float) -> float:
    """Truncate a quantity down to a whole multiple of the symbol step size."""
    if step <= 0:
        return float(qty)
    return math.floor(qty / step) * step


def daily_vol_wear(
    sym: str,
    k: str,
    s: str,
    *,
    window_start: datetime,
    window_end: datetime,
    now: datetime | None = None,
) -> dict[str, float | int | str]:
    """Return deduplicated exchange truth for one immutable run window."""

    if window_start.tzinfo is None or window_start.utcoffset() is None:
        raise ValueError("window_start must include timezone information")
    if window_end.tzinfo is None or window_end.utcoffset() is None:
        raise ValueError("window_end must include timezone information")
    start = window_start.astimezone(timezone.utc)
    end = window_end.astimezone(timezone.utc)
    if start >= end:
        raise ValueError("window_start must be earlier than window_end")
    observed_at = (now or _now()).astimezone(timezone.utc)
    query_end = min(observed_at, end)
    start_ms = int(start.timestamp() * 1000)
    end_exclusive_ms = int(query_end.timestamp() * 1000)
    if end_exclusive_ms <= start_ms:
        return {
            "gross_notional": 0.0,
            "realized_pnl": 0.0,
            "wear_per_10k": 0.0,
            "trade_count": 0,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "query_end": query_end.isoformat(),
        }

    trades = fetch_exact_futures_trade_window(
        fetch_page=lambda range_start_ms, range_end_inclusive_ms, limit: (
            fetch_futures_user_trades(
                symbol=sym,
                api_key=k,
                api_secret=s,
                start_time_ms=range_start_ms,
                end_time_ms=range_end_inclusive_ms,
                limit=limit,
                use_cache=False,
            )
        ),
        start_time_ms=start_ms,
        end_time_exclusive_ms=end_exclusive_ms,
        page_limit=1000,
        max_requests=80,
    )
    gross = 0.0
    realized_pnl = 0.0
    for trade in trades:
        try:
            if "quoteQty" in trade:
                quote_notional = float(trade["quoteQty"])
            elif "quote_qty" in trade:
                quote_notional = float(trade["quote_qty"])
            else:
                quote_notional = 0.0
            if abs(quote_notional) <= 0.0:
                price_raw = trade.get("price")
                quantity_raw = (
                    trade.get("qty")
                    if "qty" in trade
                    else trade.get("quantity")
                )
                if (
                    price_raw is None
                    or price_raw == ""
                    or quantity_raw is None
                    or quantity_raw == ""
                ):
                    raise ValueError("exchange trade notional is missing")
                price = float(price_raw)
                quantity = float(quantity_raw)
                if price <= 0.0 or abs(quantity) <= 0.0:
                    raise ValueError("exchange trade notional is missing")
                quote_notional = price * quantity
            if "realizedPnl" in trade:
                trade_realized = float(trade["realizedPnl"])
            elif "realized_pnl" in trade:
                trade_realized = float(trade["realized_pnl"])
            else:
                raise ValueError("exchange trade realizedPnl is required")
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ValueError) and str(exc).startswith("exchange trade"):
                raise
            raise ValueError("exchange trade numeric field is invalid") from exc
        if not math.isfinite(quote_notional) or not math.isfinite(trade_realized):
            raise ValueError("exchange trade numeric field must be finite")
        if abs(quote_notional) <= 0.0:
            raise ValueError("exchange trade notional must be positive")
        gross += abs(quote_notional)
        realized_pnl += trade_realized
    return {
        "gross_notional": gross,
        "realized_pnl": realized_pnl,
        "wear_per_10k": (-realized_pnl / gross * 1e4) if gross > 0 else 0.0,
        "trade_count": len(trades),
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "query_end": query_end.isoformat(),
    }


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
    sym: str, k: str, s: str, workdir: str, slug: str, tp_ratio: float, tick: float,
    qty_step: float = 1.0, max_qty: float | None = None,
) -> dict[str, Any]:
    """Rest a BUY limit to take profit on kept frozen shorts.

    Price = ``min(wavg * (1 - tp_ratio), min_entry)`` floored to tick: the whole
    book closes at >= ``tp_ratio`` average profit AND no individual lot is
    underwater at the fill price (min_entry guard), so a stopped runner can never
    realize a loss on the kept frozen shorts.

    The BUY qty is clamped to ``max_qty`` (the actual short size we are keeping, so
    the TP can never over-buy and flip the position long) and truncated down to the
    symbol ``qty_step``. The clientOrderId carries the symbol so two symbols placing
    a TP in the same minute cannot collide.
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
    # Never place a BUY larger than the short we actually hold (would flip to long).
    target_qty = tot_q if max_qty is None else min(tot_q, max(max_qty, 0.0))
    qty = _truncate_step(target_qty, qty_step)
    qty = int(qty) if float(qty).is_integer() else round(qty, 8)
    if qty <= 0 or tp_price <= 0:
        return {"placed": False, "reason": "bad_qty_or_price", "qty": qty, "price": tp_price}
    coid = (FROZEN_TP_PREFIX + slug + _now().strftime("%Y%m%d%H%M%S"))[:36]
    try:
        r = post_futures_order(
            symbol=sym, side="BUY", quantity=qty, price=tp_price, api_key=k, api_secret=s,
            time_in_force="GTC", position_side="SHORT", new_client_order_id=coid,
        )
        return {"placed": True, "qty": qty, "price": tp_price, "wavg": round(wavg, 5),
                "min_entry": min_entry, "orderId": r.get("orderId"), "coid": coid}
    except Exception as e:  # noqa: BLE001 - report the exchange error, keep going
        return {"placed": False, "reason": str(e)[:140]}


def _active_wavg(workdir: str, slug: str) -> tuple[float, float]:
    """(long_wavg, short_wavg) of the active best-quote ledger; 0.0 when unknown."""
    try:
        st = json.load(open(os.path.join(workdir, "output", f"{slug}_loop_state.json")))
        led = st.get("best_quote_volume_ledger") or {}
        out: list[float] = []
        for side in ("long_lots", "short_lots"):
            lots = led.get(side) or []
            q = sum(float(l.get("qty", 0) or 0) for l in lots)
            c = sum(float(l.get("qty", 0) or 0) * float(l.get("price", 0) or 0) for l in lots)
            out.append((c / q) if q > 0 else 0.0)
        return out[0], out[1]
    except Exception:
        return 0.0, 0.0


def resolve_flatten_action(side: str, close_qty: float, mark: float, wavg: float) -> str:
    """'market' | 'rest_tp' | 'skip' for one managed side at target/wear stop.

    An underwater side (mark below cost for LONG / above cost for SHORT) is never
    market-closed; it keeps the position and rests a breakeven TP instead. With no
    ledger cost or no mark available we fall back to the old market-flatten.
    """
    if close_qty <= 0:
        return "skip"
    if wavg <= 0 or mark <= 0:
        return "market"
    if side == "LONG":
        return "market" if mark >= wavg else "rest_tp"
    return "market" if mark <= wavg else "rest_tp"


def place_active_tp(
    sym: str, k: str, s: str, slug: str, side: str, qty: float, wavg: float,
    ratio: float, tick: float, qty_step: float = 1.0,
) -> dict[str, Any]:
    """Rest a breakeven(+ratio) reduce LIMIT for a kept underwater managed side."""
    if side == "LONG":
        raw = wavg * (1.0 + max(ratio, 0.0))
        price = round(math.ceil(raw / tick) * tick, 8)
        order_side = "SELL"
    else:
        raw = wavg * (1.0 - max(ratio, 0.0))
        price = round(math.floor(raw / tick) * tick, 8)
        order_side = "BUY"
    q = _truncate_step(qty, qty_step)
    q = int(q) if float(q).is_integer() else round(q, 8)
    if q <= 0 or price <= 0:
        return {"placed": False, "reason": "bad_qty_or_price", "qty": q, "price": price}
    coid = (ACTIVE_TP_PREFIX + side[0] + slug + _now().strftime("%Y%m%d%H%M%S"))[:36]
    try:
        r = post_futures_order(
            symbol=sym, side=order_side, quantity=q, price=price, api_key=k, api_secret=s,
            time_in_force="GTC", position_side=side, new_client_order_id=coid,
        )
        return {"placed": True, "qty": q, "price": price, "wavg": round(wavg, 6),
                "orderId": r.get("orderId"), "coid": coid}
    except Exception as e:  # noqa: BLE001 - report the exchange error, keep going
        return {"placed": False, "reason": str(e)[:140]}


def cancel_frozen_tp(sym: str, k: str, s: str) -> int:
    """Cancel any resting gate TP orders (frozen FROZENTP* and active TGTTP*)."""
    n = 0
    try:
        for o in fetch_futures_open_orders(sym, k, s):
            if str(o.get("clientOrderId", "")).startswith((FROZEN_TP_PREFIX, ACTIVE_TP_PREFIX)):
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


def evaluate_triggers(
    vol: float,
    wear: float,
    target: float,
    first: float | None,
    wear_stop: float | None,
) -> tuple[bool, bool, bool]:
    """Return (target_ok, hit_target, hit_wear).

    ``target_ok`` is False for a missing/zero/negative target; in that case
    ``hit_target`` is forced False so a 0 target can never submit a drain intent
    on a vol>=0 read.  Wear is disabled unless both immutable thresholds exist.
    """
    target_ok = target > 0
    hit_target = target_ok and vol >= target
    hit_wear = (
        first is not None
        and wear_stop is not None
        and vol >= first
        and wear > wear_stop
    )
    return target_ok, hit_target, hit_wear


def _terminal_intent_path(workdir: str, slug: str) -> Path:
    return Path(workdir) / "output" / f"{slug}_terminal_intent.json"


def _intent_id(*, symbol: str, trigger_reason: str, run_contract_id: str) -> str:
    """Build a retry-stable identity for one immutable run contract."""
    return terminal_intent_id(
        symbol=symbol,
        source=LIFECYCLE_INTENT_SOURCE,
        trigger_reason=trigger_reason,
        run_contract_id=run_contract_id,
    )


_RUN_CONTRACT_ARG_FIELDS = {
    "--symbol": "symbol",
    "--strategy-profile": "strategy_profile",
    "--strategy-mode": "strategy_mode",
    "--per-order-notional": "per_order_notional",
    "--run-start-time": "run_start_time",
    "--run-end-time": "run_end_time",
    "--runtime-guard-stats-start-time": "runtime_guard_stats_start_time",
    "--max-cumulative-notional": "max_cumulative_notional",
    "--lifecycle-wear-stop-per-10k": "lifecycle_wear_stop_per_10k",
    "--lifecycle-wear-stop-min-gross-notional": (
        "lifecycle_wear_stop_min_gross_notional"
    ),
    "--terminal-drain-exit-policy": "terminal_drain_exit_policy",
    "--terminal-drain-absolute-loss-budget": "terminal_drain_absolute_loss_budget",
    "--terminal-drain-max-wait-seconds": "terminal_drain_max_wait_seconds",
    "--terminal-drain-stop-preserve-reason": "terminal_drain_stop_preserve_reason",
    "--terminal-drain-max-order-notional": "terminal_drain_max_order_notional",
    "--terminal-drain-loss-lease-seconds": "terminal_drain_loss_lease_seconds",
    "--terminal-drain-order-reprice-seconds": "terminal_drain_order_reprice_seconds",
    "--terminal-drain-flat-confirm-cycles": "terminal_drain_flat_confirm_cycles",
}


def _run_contract_config_from_command(args_text: str) -> dict[str, Any]:
    tokens = shlex.split(str(args_text or ""))
    if "grid_optimizer.loop_runner" not in tokens:
        raise ValueError("live process is not grid_optimizer.loop_runner")
    config: dict[str, Any] = {}
    index = 0
    while index < len(tokens):
        field = _RUN_CONTRACT_ARG_FIELDS.get(tokens[index])
        if field is None or index + 1 >= len(tokens):
            index += 1
            continue
        config[field] = tokens[index + 1]
        index += 2
    return config


def load_live_runner_contract(*, workdir: str, slug: str) -> str:
    """Read the contract from the actual loop process, never from saved control."""

    output_dir = Path(workdir) / "output"
    pid_candidates = [
        output_dir / f"{slug}_loop_runner.pid",
        output_dir / "night_loop_runner.pid",
    ]
    pid = 0
    for pid_path in pid_candidates:
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            continue
        if pid > 0:
            break
    if pid <= 0:
        raise ValueError("live runner pid is unavailable")
    proc = subprocess.run(
        ["ps", "-ww", "-p", str(pid), "-o", "args="],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0 or not str(proc.stdout or "").strip():
        raise ValueError("live runner command is unavailable")
    config = _run_contract_config_from_command(str(proc.stdout).strip().splitlines()[-1])
    return run_contract_identity_from_config(config)


def _parse_intent_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _completed_intent_belongs_to_older_run(
    existing: dict[str, Any],
    *,
    run_contract_id: str,
    run_start_time: str | None,
) -> bool:
    if existing.get("status") not in LIFECYCLE_INTENT_COMPLETED_STATUSES:
        return False
    existing_contract_id = str(existing.get("run_contract_id") or "").strip()
    if existing_contract_id:
        return existing_contract_id != run_contract_id
    requested_at = _parse_intent_timestamp(existing.get("requested_at"))
    current_start = _parse_intent_timestamp(run_start_time)
    return bool(requested_at and current_start and requested_at < current_start)


def _archive_completed_intent(path: Path, existing: dict[str, Any]) -> None:
    history_dir = path.parent / "terminal_intent_history"
    history_dir.mkdir(parents=True, exist_ok=True)
    intent_id = str(existing.get("intent_id") or "unknown")
    safe_id = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in intent_id)
    archive_path = history_dir / f"{safe_id or 'unknown'}.json"
    os.replace(path, archive_path)


def submit_lifecycle_intent(
    *,
    workdir: str,
    symbol: str,
    trigger_reason: str,
    requested_at: str,
    observed: dict[str, Any],
    run_contract_config: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Atomically persist an idempotent terminal-drain intent.

    Returns ``(intent, created)``.  An existing valid intent is never overwritten:
    its owner/consumer must finish and archive it before another run contract can
    claim the symbol.
    """
    slug = symbol.lower()
    path = _terminal_intent_path(workdir, slug)
    run_contract_snapshot, run_contract_id = resolve_authoritative_run_contract(
        run_contract_config,
        expected_symbol=symbol,
    )
    if run_contract_snapshot.get("symbol") != symbol:
        raise ValueError("lifecycle intent symbol does not match run contract")
    run_start_time = str(run_contract_snapshot.get("run_start_time") or "") or None

    def load_existing() -> dict[str, Any]:
        existing = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(existing, dict):
            raise ValueError(f"invalid lifecycle intent payload: {path}")
        if (
            existing.get("schema") != LIFECYCLE_INTENT_SCHEMA
            or existing.get("symbol") != symbol
            or existing.get("action") != "lifecycle_drain"
        ):
            raise ValueError(f"conflicting lifecycle intent: {path}")
        return existing

    intent: dict[str, Any] = {
        "schema": LIFECYCLE_INTENT_SCHEMA,
        "intent_id": _intent_id(
            symbol=symbol,
            trigger_reason=trigger_reason,
            run_contract_id=run_contract_id,
        ),
        "symbol": symbol,
        "source": LIFECYCLE_INTENT_SOURCE,
        "action": "lifecycle_drain",
        "trigger_reason": trigger_reason,
        "requested_at": requested_at,
        "status": "pending",
        "exit_policy": "use_immutable_run_contract",
        "run_contract_id": run_contract_id,
        "run_contract_snapshot": run_contract_snapshot,
        "observed": observed,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with exclusive_control_lock(path):
        if path.exists():
            existing = load_existing()
            if _completed_intent_belongs_to_older_run(
                existing,
                run_contract_id=run_contract_id,
                run_start_time=run_start_time,
            ):
                _archive_completed_intent(path, existing)
            else:
                return existing, False

        temp_path = path.with_name(
            f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
        )
        try:
            with temp_path.open("x", encoding="utf-8") as handle:
                json.dump(intent, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.link(temp_path, path)
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
    return intent, True


def confirm_stopped(service: str, retries: int = 6, delay: float = 1.0) -> bool:
    """Return True only once systemctl reports the service inactive.

    The gate must never cancel orders or market-flatten while the runner might
    still be trading, so the caller aborts if this returns False.
    """
    for _ in range(max(retries, 1)):
        state = subprocess.run(
            ["systemctl", "is-active", service], capture_output=True, text=True
        ).stdout.strip()
        if state in ("inactive", "failed"):
            return True
        time.sleep(delay)
    return False


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--service", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument(
        "--target",
        type=float,
        default=0.0,
        help="Deprecated report-only input; enforcement always uses the live immutable run contract.",
    )
    ap.add_argument(
        "--wear-stop",
        type=float,
        default=None,
        help="Deprecated report-only input; immutable runner control is authoritative.",
    )
    ap.add_argument(
        "--first",
        type=float,
        default=None,
        help="Deprecated report-only input; immutable runner control is authoritative.",
    )
    ap.add_argument("--qty-step", type=float, default=1.0,
                    help="Symbol lot step; the frozen-short TP qty is truncated to it.")
    ap.add_argument("--plan-json", default=None,
                    help="Defaults to output/<symbol>_loop_latest_plan.json.")
    ap.add_argument("--tp-ratio", type=float, default=0.05)
    ap.add_argument("--active-tp-ratio", type=float, default=0.001,
                    help="Breakeven margin for the kept underwater managed side's resting TP.")
    ap.add_argument("--tick", type=float, default=0.0001)
    ap.add_argument("--place-tp-now", action="store_true",
                    help="Rest the frozen-short TP for an already-stopped runner, then exit.")
    ap.add_argument("--enforce", action="store_true")
    return ap


def main() -> None:
    a = build_parser().parse_args()
    sym = a.symbol.upper()
    slug = sym.lower()
    k = os.environ["BINANCE_API_KEY"]
    s = os.environ["BINANCE_API_SECRET"]

    # The saved control is the only source of the immutable target window and
    # exit contract.  CLI target fallback is intentionally not an enforcement
    # authority because it cannot identify which run owns the observed volume.
    config_ok = False
    cfg: dict[str, Any] = {}
    try:
        loaded_cfg = json.load(open(os.path.join(a.workdir, "output", f"{slug}_loop_runner_control.json")))
        if not isinstance(loaded_cfg, dict):
            raise ValueError("runner control must be a JSON object")
        cfg = loaded_cfg
        config_ok = True
    except Exception:
        config_ok = False

    now = _now()
    ts = now.isoformat()

    # Manual: rest the frozen-short TP for an already-stopped runner, then exit.
    if a.place_tp_now:
        info = (
            place_frozen_short_tp(sym, k, s, a.workdir, slug, a.tp_ratio, a.tick, a.qty_step)
            if a.enforce else {"dry_run": True}
        )
        print(json.dumps({"ts": ts, "place_tp_now": info}))
        return

    if not config_ok:
        print(
            json.dumps(
                {
                    "ts": ts,
                    "target": 0.0,
                    "hit_target": False,
                    "hit_wear": False,
                    "enforce": a.enforce,
                    "action": "LIFECYCLE_INTENT_REJECTED",
                    "config_error": "missing_config",
                }
            )
        )
        return

    try:
        run_contract = validate_run_contract(
            run_start_time=cfg.get("run_start_time"),
            runtime_guard_stats_start_time=cfg.get(
                "runtime_guard_stats_start_time"
            ),
            run_end_time=cfg.get("run_end_time"),
            target_value=cfg.get("max_cumulative_notional"),
            exit_policy=cfg.get("terminal_drain_exit_policy"),
            loss_budget=cfg.get("terminal_drain_absolute_loss_budget"),
            max_wait_seconds=cfg.get("terminal_drain_max_wait_seconds"),
            preserve_reason=cfg.get("terminal_drain_stop_preserve_reason"),
            wear_stop_per_10k=cfg.get("lifecycle_wear_stop_per_10k"),
            wear_stop_min_gross_notional=cfg.get(
                "lifecycle_wear_stop_min_gross_notional"
            ),
        )
        if (
            run_contract.runtime_guard_stats_start_time is None
            or run_contract.run_end_time is None
        ):
            raise ValueError(
                "target/wear gate requires runtime_guard_stats_start_time and run_end_time"
            )
    except ValueError as exc:
        print(
            json.dumps(
                {
                    "ts": ts,
                    "target": a.target,
                    "hit_target": False,
                    "hit_wear": False,
                    "enforce": a.enforce,
                    "action": "LIFECYCLE_INTENT_REJECTED",
                    "config_error": "invalid_run_contract",
                    "error": str(exc),
                }
            )
        )
        return

    try:
        authoritative_snapshot, control_contract_id = (
            resolve_authoritative_run_contract(
                cfg,
                expected_symbol=sym,
            )
        )
    except ValueError as exc:
        print(
            json.dumps(
                {
                    "ts": ts,
                    "target": a.target,
                    "hit_target": False,
                    "hit_wear": False,
                    "enforce": a.enforce,
                    "action": "LIFECYCLE_INTENT_REJECTED",
                    "config_error": "invalid_run_contract_owner",
                    "error": str(exc),
                }
            )
        )
        return

    # Rebuild all enforcement terms from the owner's canonical snapshot. CLI
    # wear inputs remain accepted only so old scripts do not fail to parse.
    run_contract = validate_run_contract(
        run_start_time=authoritative_snapshot.get("run_start_time"),
        runtime_guard_stats_start_time=authoritative_snapshot.get(
            "runtime_guard_stats_start_time"
        ),
        run_end_time=authoritative_snapshot.get("run_end_time"),
        target_value=authoritative_snapshot.get("max_cumulative_notional"),
        exit_policy=authoritative_snapshot.get("terminal_drain_exit_policy"),
        loss_budget=authoritative_snapshot.get(
            "terminal_drain_absolute_loss_budget"
        ),
        max_wait_seconds=authoritative_snapshot.get(
            "terminal_drain_max_wait_seconds"
        ),
        preserve_reason=authoritative_snapshot.get(
            "terminal_drain_stop_preserve_reason"
        ),
        wear_stop_per_10k=authoritative_snapshot.get(
            "lifecycle_wear_stop_per_10k"
        ),
        wear_stop_min_gross_notional=authoritative_snapshot.get(
            "lifecycle_wear_stop_min_gross_notional"
        ),
    )
    a.target = float(run_contract.target_value or 0.0)
    wear_stop = run_contract.wear_stop_per_10k
    wear_first = run_contract.wear_stop_min_gross_notional

    try:
        window_stats = daily_vol_wear(
            sym,
            k,
            s,
            window_start=run_contract.runtime_guard_stats_start_time,
            window_end=run_contract.run_end_time,
            now=now,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "ts": ts,
                    "target": a.target,
                    "hit_target": False,
                    "hit_wear": False,
                    "enforce": a.enforce,
                    "action": "LIFECYCLE_INTENT_REJECTED",
                    "config_error": "trade_query_incomplete",
                    "run_contract_id": control_contract_id,
                    "error": str(exc),
                }
            )
        )
        return

    vol = float(window_stats["gross_notional"])
    wear = float(window_stats["wear_per_10k"])
    target_ok, hit_target, hit_wear = evaluate_triggers(
        vol,
        wear,
        a.target,
        wear_first,
        wear_stop,
    )
    status: dict[str, Any] = {
        "ts": ts, "vol": round(vol, 0), "wear": round(wear, 2), "target": a.target,
        "hit_target": hit_target, "hit_wear": hit_wear, "enforce": a.enforce,
        "run_contract_id": control_contract_id,
        "window_start": window_stats["window_start"],
        "window_end": window_stats["window_end"],
        "query_end": window_stats["query_end"],
        "trade_count": int(window_stats["trade_count"]),
        "realized_pnl": float(window_stats["realized_pnl"]),
        "remaining_target": max(float(a.target) - vol, 0.0),
    }
    if not target_ok and wear_stop is None:
        status["config_error"] = "missing_config" if not config_ok else "non_positive_target"
    if not (hit_target or hit_wear):
        print(json.dumps(status))
        return
    status["trigger"] = "target" if hit_target else "wear"
    if not a.enforce:
        status["action"] = "DRY_RUN_would_submit_lifecycle_intent"
        print(json.dumps(status))
        return

    try:
        live_contract_id = load_live_runner_contract(workdir=a.workdir, slug=slug)
    except (OSError, TypeError, ValueError) as exc:
        status.update(
            {
                "action": "LIFECYCLE_INTENT_REJECTED",
                "config_error": "live_run_contract_unavailable",
                "error": str(exc),
            }
        )
        print(json.dumps(status))
        return
    if live_contract_id != control_contract_id:
        status.update(
            {
                "action": "LIFECYCLE_INTENT_REJECTED",
                "config_error": "live_run_contract_mismatch",
                "control_run_contract_id": control_contract_id,
                "live_run_contract_id": live_contract_id,
            }
        )
        print(json.dumps(status))
        return

    observed = {
        "gross_notional": float(vol),
        "realized_pnl": float(window_stats["realized_pnl"]),
        "wear_per_10k": float(wear),
        "trade_count": int(window_stats["trade_count"]),
        "target": float(a.target),
        "first": wear_first,
        "wear_stop": wear_stop,
        "window_start": str(window_stats["window_start"]),
        "window_end": str(window_stats["window_end"]),
        "query_end": str(window_stats["query_end"]),
    }
    trigger_reason = "target_reached" if hit_target else "wear_limit_breached"
    try:
        intent, created = submit_lifecycle_intent(
            workdir=a.workdir,
            symbol=sym,
            trigger_reason=trigger_reason,
            requested_at=ts,
            observed=observed,
            run_contract_config={
                **cfg,
                # The resolver above is the authority.  Reattach its exact
                # snapshot fields before the idempotent submitter revalidates.
                **authoritative_snapshot,
            },
        )
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        status["action"] = "LIFECYCLE_INTENT_SUBMISSION_FAILED"
        status["error"] = str(exc)
        print(json.dumps(status))
        return
    status.update(
        {
            "action": (
                "LIFECYCLE_INTENT_SUBMITTED"
                if created
                else "LIFECYCLE_INTENT_ALREADY_PENDING"
            ),
            "intent_id": intent["intent_id"],
            "intent_path": str(_terminal_intent_path(a.workdir, slug)),
        }
    )
    print(json.dumps(status))


if __name__ == "__main__":
    main()
