from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data import (
    delete_futures_order,
    delete_spot_order,
    fetch_futures_open_orders,
    fetch_futures_position_risk_v3,
    fetch_spot_account_info,
    fetch_spot_book_tickers,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    fetch_spot_user_trades,
    load_binance_api_credentials,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _append_event(path: Path, event: str, **fields: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"ts": _now_iso(), "event": event, **fields}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def cleanup_trigger(
    *,
    volume: float,
    target: float,
    armed: bool,
    runner_active: bool,
    inactive_since: float | None,
    now_monotonic: float,
    inactive_grace_seconds: float,
) -> str | None:
    if target > 0 and volume >= target:
        return "target_reached"
    if armed and not runner_active and inactive_since is not None:
        if now_monotonic - inactive_since >= max(inactive_grace_seconds, 0.0):
            return "runner_stopped"
    return None


def _runner_active(service: str) -> bool:
    result = subprocess.run(
        ["systemctl", "is-active", service],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() == "active"


def _stop_runner(wrapper: str, symbol: str, service: str) -> None:
    subprocess.run([wrapper, "stop", symbol], check=False, capture_output=True, text=True, timeout=90)
    for _ in range(30):
        if not _runner_active(service):
            return
        time.sleep(1)
    raise RuntimeError(f"runner service did not stop: {service}")


def _cancel_symbol_orders(symbol: str, api_key: str, api_secret: str) -> dict[str, int]:
    canceled_spot = 0
    canceled_futures = 0
    for order in fetch_spot_open_orders(symbol, api_key, api_secret):
        delete_spot_order(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            order_id=int(order["orderId"]),
        )
        canceled_spot += 1
    for order in fetch_futures_open_orders(symbol, api_key, api_secret, use_cache=False):
        delete_futures_order(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            order_id=int(order["orderId"]),
        )
        canceled_futures += 1
    return {"spot": canceled_spot, "futures": canceled_futures}


def _spot_inventory(api_key: str, api_secret: str, base_asset: str) -> tuple[float, float]:
    account = fetch_spot_account_info(api_key, api_secret)
    for row in account.get("balances", []):
        if str(row.get("asset", "")).upper() == base_asset:
            return _safe_float(row.get("free")), _safe_float(row.get("locked"))
    return 0.0, 0.0


def _futures_position_qty(symbol: str, api_key: str, api_secret: str) -> tuple[float, float]:
    long_qty = 0.0
    short_qty = 0.0
    for row in fetch_futures_position_risk_v3(api_key, api_secret, symbol=symbol):
        side = str(row.get("positionSide", "BOTH")).upper()
        qty = _safe_float(row.get("positionAmt"))
        if side == "LONG":
            long_qty += abs(qty)
        elif side == "SHORT":
            short_qty += abs(qty)
        elif qty > 0:
            long_qty += qty
        elif qty < 0:
            short_qty += abs(qty)
    return long_qty, short_qty


def _update_trade_progress(
    *,
    state: dict[str, Any],
    symbol: str,
    api_key: str,
    api_secret: str,
    start_ms: int,
) -> dict[str, Any]:
    last_id = int(state.get("last_trade_id", -1))
    cursor_ms = max(int(state.get("last_trade_time_ms", start_ms)), start_ms)
    gross = _safe_float(state.get("gross_notional"))
    trade_count = int(state.get("trade_count", 0))
    commission_quote = _safe_float(state.get("commission_quote"))

    for _ in range(100):
        rows = fetch_spot_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=cursor_ms,
            limit=1000,
        )
        fresh = [row for row in rows if int(row.get("id", -1)) > last_id and int(row.get("time", 0)) >= start_ms]
        if not fresh:
            break
        fresh.sort(key=lambda row: (int(row.get("time", 0)), int(row.get("id", -1))))
        for row in fresh:
            gross += _safe_float(row.get("quoteQty"))
            trade_count += 1
            if str(row.get("commissionAsset", "")).upper() == "USDT":
                commission_quote += _safe_float(row.get("commission"))
            last_id = max(last_id, int(row.get("id", -1)))
            cursor_ms = max(cursor_ms, int(row.get("time", cursor_ms)))
        if len(rows) < 1000:
            break

    return {
        **state,
        "gross_notional": gross,
        "trade_count": trade_count,
        "commission_quote": commission_quote,
        "last_trade_id": last_id,
        "last_trade_time_ms": cursor_ms,
        "updated_at": _now_iso(),
    }


def _run_flatteners(args: argparse.Namespace, state: dict[str, Any], events_path: Path) -> dict[str, Any]:
    python_bin = str(args.python_bin)
    state["phase"] = "flatten_spot"
    state["updated_at"] = _now_iso()
    _write_json(Path(args.state), state)
    _append_event(events_path, "flatten_spot_started")
    subprocess.run(
        [
            python_bin,
            "-m",
            "grid_optimizer.spot_flatten_runner",
            "--symbol",
            args.symbol,
            "--client-order-prefix",
            args.spot_flatten_prefix,
            "--sleep-seconds",
            str(args.flatten_sleep_seconds),
            "--events-jsonl",
            str(args.spot_flatten_events),
        ],
        check=True,
    )

    state["phase"] = "flatten_futures"
    state["updated_at"] = _now_iso()
    _write_json(Path(args.state), state)
    _append_event(events_path, "flatten_futures_started")
    subprocess.run(
        [
            python_bin,
            "-m",
            "grid_optimizer.maker_flatten_runner",
            "--symbol",
            args.symbol,
            "--client-order-prefix",
            args.futures_flatten_prefix,
            "--sleep-seconds",
            str(args.flatten_sleep_seconds),
            "--events-jsonl",
            str(args.futures_flatten_events),
            "--allow-loss",
        ],
        check=True,
    )
    return state


def _verify_flat(
    *,
    args: argparse.Namespace,
    api_key: str,
    api_secret: str,
) -> dict[str, Any]:
    symbol_info = fetch_spot_symbol_config(args.symbol)
    base_asset = str(symbol_info.get("base_asset", "")).upper()
    min_notional = _safe_float(symbol_info.get("min_notional"))
    book = fetch_spot_book_tickers(args.symbol)
    bid = _safe_float(book[0].get("bid_price")) if book else 0.0
    free, locked = _spot_inventory(api_key, api_secret, base_asset)
    long_qty, short_qty = _futures_position_qty(args.symbol, api_key, api_secret)
    spot_orders = fetch_spot_open_orders(args.symbol, api_key, api_secret)
    futures_orders = fetch_futures_open_orders(args.symbol, api_key, api_secret, use_cache=False)
    spot_qty = free + locked
    spot_dust = spot_qty > 0 and (bid <= 0 or spot_qty * bid < min_notional)
    flat = (
        (spot_qty <= 0 or spot_dust)
        and long_qty <= 0
        and short_qty <= 0
        and not spot_orders
        and not futures_orders
    )
    return {
        "flat": flat,
        "spot_qty": spot_qty,
        "spot_notional": spot_qty * bid,
        "spot_dust": spot_dust,
        "futures_long_qty": long_qty,
        "futures_short_qty": short_qty,
        "spot_open_orders": len(spot_orders),
        "futures_open_orders": len(futures_orders),
    }


def _run(args: argparse.Namespace) -> int:
    creds = load_binance_api_credentials()
    if creds is None:
        raise RuntimeError("missing Binance API credentials")
    api_key, api_secret = creds
    state_path = Path(args.state)
    events_path = Path(args.events)
    state = _read_json(state_path)
    if state and (
        int(state.get("start_ms", 0)) != int(args.start_ms)
        or _safe_float(state.get("target_volume")) != float(args.target_volume)
    ):
        state = {}
    if state.get("phase") == "complete":
        return 0
    state.setdefault("phase", "monitoring")
    state.setdefault("armed", False)
    state.setdefault("gross_notional", 0.0)
    state.setdefault("trade_count", 0)
    state.setdefault("commission_quote", 0.0)
    state.setdefault("start_ms", int(args.start_ms))
    state.setdefault("target_volume", float(args.target_volume))
    state.setdefault("started_at", _now_iso())
    state.setdefault("started_epoch", time.time())
    _write_json(state_path, state)

    inactive_since: float | None = None
    while state.get("phase") == "monitoring":
        state = _update_trade_progress(
            state=state,
            symbol=args.symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_ms=int(args.start_ms),
        )
        active = _runner_active(args.service)
        if active:
            state["armed"] = True
            inactive_since = None
        elif bool(state.get("armed")):
            inactive_since = inactive_since or time.monotonic()
        reason = cleanup_trigger(
            volume=_safe_float(state.get("gross_notional")),
            target=float(args.target_volume),
            armed=bool(state.get("armed")),
            runner_active=active,
            inactive_since=inactive_since,
            now_monotonic=time.monotonic(),
            inactive_grace_seconds=float(args.inactive_grace_seconds),
        )
        if not reason and not bool(state.get("armed")):
            if time.time() - _safe_float(state.get("started_epoch")) >= float(args.startup_grace_seconds):
                reason = "runner_start_timeout"
        state["runner_active"] = active
        state["remaining"] = max(float(args.target_volume) - _safe_float(state.get("gross_notional")), 0.0)
        _write_json(state_path, state)
        if reason:
            state["phase"] = "stopping"
            state["trigger_reason"] = reason
            state["triggered_at"] = _now_iso()
            _write_json(state_path, state)
            _append_event(events_path, "cleanup_triggered", reason=reason, gross_notional=state["gross_notional"])
            break
        time.sleep(max(float(args.poll_seconds), 1.0))

    if state.get("phase") in {"stopping", "flatten_spot", "flatten_futures", "verifying"}:
        _stop_runner(args.wrapper, args.symbol, args.service)
        canceled = _cancel_symbol_orders(args.symbol, api_key, api_secret)
        _append_event(events_path, "runner_stopped_orders_canceled", canceled=canceled)
        state = _run_flatteners(args, state, events_path)
        state["phase"] = "verifying"
        state["updated_at"] = _now_iso()
        _write_json(state_path, state)

    confirmations = 0
    while confirmations < int(args.flat_confirm_cycles):
        snapshot = _verify_flat(args=args, api_key=api_key, api_secret=api_secret)
        state["final_snapshot"] = snapshot
        state["updated_at"] = _now_iso()
        _write_json(state_path, state)
        confirmations = confirmations + 1 if snapshot["flat"] else 0
        if not snapshot["flat"]:
            raise RuntimeError(f"terminal verification not flat: {snapshot}")
        if confirmations < int(args.flat_confirm_cycles):
            time.sleep(max(float(args.poll_seconds), 1.0))

    state["phase"] = "complete"
    state["completed_at"] = _now_iso()
    state["runner_active"] = False
    _write_json(state_path, state)
    _append_event(events_path, "cleanup_complete", snapshot=state.get("final_snapshot"))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autonomous terminal guard for spot competitions with a static futures hedge.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--target-volume", type=float, required=True)
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--wrapper", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--python-bin", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--spot-flatten-events", required=True)
    parser.add_argument("--futures-flatten-events", required=True)
    parser.add_argument("--spot-flatten-prefix", default="sctgspot")
    parser.add_argument("--futures-flatten-prefix", default="sctgfut")
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--flatten-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--inactive-grace-seconds", type=float, default=30.0)
    parser.add_argument("--startup-grace-seconds", type=float, default=300.0)
    parser.add_argument("--flat-confirm-cycles", type=int, default=3)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    args.symbol = str(args.symbol).upper().strip()
    if args.target_volume <= 0:
        raise SystemExit("--target-volume must be > 0")
    if args.start_ms <= 0:
        raise SystemExit("--start-ms must be > 0")
    if args.flat_confirm_cycles <= 0:
        raise SystemExit("--flat-confirm-cycles must be > 0")
    raise SystemExit(_run(args))


if __name__ == "__main__":
    main()
