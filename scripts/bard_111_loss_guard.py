#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SYMBOL = "BARDUSDT"
WINDOW_SECONDS = 3600
MAX_LOSS_PER_10K = 6.0
MIN_GROSS_TO_ENFORCE = 1000.0
ROOT = Path("/home/ubuntu/wangge")
OUT = ROOT / "output"
EVENTS = OUT / "bardusdt_loop_events.jsonl"
TRADES = OUT / "bardusdt_loop_trade_audit.jsonl"
INCOME = OUT / "bardusdt_loop_income_audit.jsonl"
PID_PATH = OUT / "bardusdt_loop_runner.pid"
REPORT_PATH = OUT / "bardusdt_loss_guard_report.json"
HISTORY_PATH = OUT / "bardusdt_loss_guard_history.jsonl"
ALERT_PATH = OUT / "bardusdt_loss_guard_alert.json"
START_MS_PATH = OUT / "bardusdt_loss_guard_start_ms"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _symbol_output_slug(symbol: str) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(symbol or "").strip().lower()).strip("_")
    return normalized or "symbol"


def _read_jsonl(path: Path):
    if not path.exists():
        return
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _tail_json(path: Path) -> dict[str, Any] | None:
    last = None
    for obj in _read_jsonl(path) or []:
        last = obj
    return last


def _float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _guard_start_ms() -> int | None:
    try:
        value = int(START_MS_PATH.read_text().strip())
        return value if value > 0 else None
    except Exception:
        return None


def _bnb_price() -> float | None:
    try:
        from grid_optimizer.data import fetch_futures_book_tickers

        row = fetch_futures_book_tickers(symbol="BNBUSDT")[0]
        return (_float(row.get("bid_price")) + _float(row.get("ask_price"))) / 2.0
    except Exception:
        return None


def _runner_alive() -> tuple[bool, int | None]:
    return _pid_is_running(PID_PATH)


def _flatten_pid_path(symbol: str) -> Path:
    return OUT / f"{_symbol_output_slug(symbol)}_maker_flatten.pid"


def _flatten_log_path(symbol: str) -> Path:
    return OUT / f"{_symbol_output_slug(symbol)}_maker_flatten.log"


def _flatten_events_path(symbol: str) -> Path:
    return OUT / f"{_symbol_output_slug(symbol)}_maker_flatten_events.jsonl"


def _pid_is_running(pid_path: Path) -> tuple[bool, int | None]:
    if not pid_path.exists():
        return False, None
    try:
        pid = int(pid_path.read_text().strip())
    except Exception:
        pid_path.unlink(missing_ok=True)
        return False, None
    try:
        os.kill(pid, 0)
        return True, pid
    except OSError:
        pid_path.unlink(missing_ok=True)
        return False, pid


def _flatten_runner_alive() -> tuple[bool, int | None]:
    return _pid_is_running(_flatten_pid_path(SYMBOL))


def _cancel_all_symbol_orders(api_key: str, api_secret: str, *, include_flatten_orders: bool = True) -> dict[str, Any]:
    from grid_optimizer.data import delete_futures_order, fetch_futures_open_orders
    from grid_optimizer.maker_flatten_runner import flatten_client_order_prefix, is_flatten_order

    orders = fetch_futures_open_orders(symbol=SYMBOL, api_key=api_key, api_secret=api_secret)
    flatten_prefix = flatten_client_order_prefix(SYMBOL)
    target_orders = []
    skipped_flatten_orders = 0
    for order in orders:
        if not include_flatten_orders and is_flatten_order(order, flatten_prefix):
            skipped_flatten_orders += 1
            continue
        target_orders.append(order)
    errors = []
    canceled = 0
    for order in target_orders:
        try:
            delete_futures_order(
                symbol=SYMBOL,
                api_key=api_key,
                api_secret=api_secret,
                order_id=order.get("orderId"),
                orig_client_order_id=order.get("clientOrderId"),
            )
            canceled += 1
        except Exception as exc:
            errors.append({"orderId": order.get("orderId"), "error": str(exc)})
    remaining = fetch_futures_open_orders(symbol=SYMBOL, api_key=api_key, api_secret=api_secret)
    remaining_flatten = [order for order in remaining if is_flatten_order(order, flatten_prefix)]
    return {
        "attempts": len(target_orders),
        "canceled": canceled,
        "skipped_flatten_orders": skipped_flatten_orders,
        "remaining_open_order_count": len(remaining),
        "remaining_flatten_open_order_count": len(remaining_flatten),
        "remaining_non_flatten_open_order_count": len(remaining) - len(remaining_flatten),
        "errors": errors[:10],
    }


def _start_maker_flatten_process() -> dict[str, Any]:
    from grid_optimizer.maker_flatten_runner import flatten_client_order_prefix

    pid_path = _flatten_pid_path(SYMBOL)
    alive, pid = _flatten_runner_alive()
    if alive:
        return {
            "started": False,
            "already_running": True,
            "pid": pid,
            "pid_path": str(pid_path),
            "log_path": str(_flatten_log_path(SYMBOL)),
            "events_path": str(_flatten_events_path(SYMBOL)),
        }
    log_path = _flatten_log_path(SYMBOL)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    src_path = str((ROOT / "src").resolve())
    current_pythonpath = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = src_path if not current_pythonpath else f"{src_path}{os.pathsep}{current_pythonpath}"
    command = [
        sys.executable,
        "-m",
        "grid_optimizer.maker_flatten_runner",
        "--symbol",
        SYMBOL,
        "--client-order-prefix",
        flatten_client_order_prefix(SYMBOL),
        "--sleep-seconds",
        "2.0",
        "--recv-window",
        "5000",
        "--max-consecutive-errors",
        "20",
        "--events-jsonl",
        str(_flatten_events_path(SYMBOL)),
    ]
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    pid_path.write_text(str(proc.pid))
    return {
        "started": True,
        "already_running": False,
        "pid": proc.pid,
        "pid_path": str(pid_path),
        "log_path": str(log_path),
        "events_path": str(_flatten_events_path(SYMBOL)),
    }


def _stop_runner_cancel_and_flatten() -> dict[str, Any]:
    from grid_optimizer.data import load_binance_api_credentials

    actions: dict[str, Any] = {"sent_signal": False, "errors": []}
    alive, pid = _runner_alive()
    if alive and pid:
        try:
            os.kill(pid, signal.SIGTERM)
            actions["sent_signal"] = True
            actions["pid"] = pid
        except Exception as exc:
            actions["errors"].append(f"sigterm_failed:{exc}")
    for _ in range(10):
        runner_alive, _ = _runner_alive()
        if not runner_alive:
            break
        time.sleep(0.2)
    flatten_alive, flatten_pid = _flatten_runner_alive()
    actions["flatten_runner_before"] = {"alive": flatten_alive, "pid": flatten_pid}
    try:
        api_key, api_secret = load_binance_api_credentials()
        actions["cancel_result"] = _cancel_all_symbol_orders(
            api_key,
            api_secret,
            include_flatten_orders=False,
        )
        actions["maker_flatten_result"] = _start_maker_flatten_process()
        time.sleep(0.5)
        actions["post_start_exposure"] = _live_exposure_snapshot(api_key, api_secret)
        flatten_alive, flatten_pid = _flatten_runner_alive()
        actions["flatten_runner_after"] = {"alive": flatten_alive, "pid": flatten_pid}
    except Exception as exc:
        actions["errors"].append(f"flatten_exception:{exc}")
    return actions


def _live_exposure_snapshot(api_key: str, api_secret: str) -> dict[str, Any]:
    from grid_optimizer.data import fetch_futures_account_info_v3, fetch_futures_open_orders
    from grid_optimizer.live_check import extract_symbol_position
    from grid_optimizer.maker_flatten_runner import flatten_client_order_prefix, is_flatten_order

    orders = fetch_futures_open_orders(symbol=SYMBOL, api_key=api_key, api_secret=api_secret)
    flatten_prefix = flatten_client_order_prefix(SYMBOL)
    flatten_orders = [order for order in orders if is_flatten_order(order, flatten_prefix)]
    non_flatten_orders = [order for order in orders if not is_flatten_order(order, flatten_prefix)]
    position = extract_symbol_position(fetch_futures_account_info_v3(api_key, api_secret), SYMBOL)
    qty = _float(position.get("positionAmt"))
    notional = _float(position.get("notional"))
    return {
        "open_order_count": len(orders),
        "flatten_open_order_count": len(flatten_orders),
        "non_flatten_open_order_count": len(non_flatten_orders),
        "position_qty": qty,
        "position_notional": notional,
        "has_exposure": bool(non_flatten_orders) or abs(qty) > 1e-12 or abs(notional) > 1e-9,
        "position": position,
    }


def main() -> int:
    now_ms = _now_ms()
    configured_start_ms = _guard_start_ms()
    rolling_start_ms = now_ms - WINDOW_SECONDS * 1000
    start_ms = max(rolling_start_ms, configured_start_ms or 0)
    bnb = _bnb_price()
    trades = [t for t in (_read_jsonl(TRADES) or []) if int(t.get("time") or 0) >= start_ms]
    incomes = [i for i in (_read_jsonl(INCOME) or []) if int(i.get("time") or 0) >= start_ms]
    gross = sum(_float(t.get("quoteQty")) for t in trades)
    buy_gross = sum(_float(t.get("quoteQty")) for t in trades if t.get("side") == "BUY")
    sell_gross = sum(_float(t.get("quoteQty")) for t in trades if t.get("side") == "SELL")
    realized = sum(_float(t.get("realizedPnl")) for t in trades)
    income_total = sum(_float(i.get("income")) for i in incomes if i.get("asset") == "USDT")
    commission_usdt = 0.0
    commission_unknown = []
    for t in trades:
        asset = str(t.get("commissionAsset") or "")
        commission = _float(t.get("commission"))
        if commission <= 0:
            continue
        if asset == "USDT":
            commission_usdt += commission
        elif asset == "BNB" and bnb:
            commission_usdt += commission * bnb
        elif asset:
            commission_unknown.append({"asset": asset, "commission": commission})
    net_after_fee = realized + income_total - commission_usdt
    loss = max(0.0, -net_after_fee)
    loss_per_10k = (loss / gross * 10000.0) if gross > 0 else 0.0
    last_event = _tail_json(EVENTS) or {}
    alive, pid = _runner_alive()
    flatten_alive, flatten_pid = _flatten_runner_alive()
    breach = gross >= MIN_GROSS_TO_ENFORCE and loss_per_10k > MAX_LOSS_PER_10K
    stopped_exposure: dict[str, Any] | None = None
    exposure_error: str | None = None
    if not alive:
        try:
            from grid_optimizer.data import load_binance_api_credentials

            api_key, api_secret = load_binance_api_credentials()
            stopped_exposure = _live_exposure_snapshot(api_key, api_secret)
        except Exception as exc:
            exposure_error = str(exc)
    report: dict[str, Any] = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "symbol": SYMBOL,
        "window_seconds": WINDOW_SECONDS,
        "configured_start_ms": configured_start_ms,
        "effective_start_ms": start_ms,
        "gross": gross,
        "buy_gross": buy_gross,
        "sell_gross": sell_gross,
        "trade_count": len(trades),
        "sell_realized_pnl": realized,
        "income_usdt": income_total,
        "commission_usdt_estimated": commission_usdt,
        "bnb_price_used": bnb,
        "net_after_fee_estimated": net_after_fee,
        "loss_per_10k": loss_per_10k,
        "max_loss_per_10k": MAX_LOSS_PER_10K,
        "min_gross_to_enforce": MIN_GROSS_TO_ENFORCE,
        "breach": breach,
        "runner_alive": alive,
        "runner_pid": pid,
        "flatten_runner_alive": flatten_alive,
        "flatten_runner_pid": flatten_pid,
        "last_event_ts": last_event.get("ts"),
        "runtime_status": last_event.get("runtime_status"),
        "rolling_hourly_loss": last_event.get("rolling_hourly_loss"),
        "current_long_notional": last_event.get("current_long_notional"),
        "current_short_notional": last_event.get("current_short_notional"),
        "actual_net_notional": last_event.get("actual_net_notional"),
        "open_order_count": last_event.get("open_order_count"),
        "buy_paused": last_event.get("buy_paused"),
        "short_cover_paused": last_event.get("short_cover_paused"),
        "market_bias_regime": last_event.get("market_bias_regime"),
        "market_bias_score": last_event.get("market_bias_score"),
        "market_bias_strong_short_pause_active": last_event.get("market_bias_strong_short_pause_active"),
        "commission_unknown": commission_unknown[-5:],
        "stopped_exposure": stopped_exposure,
        "stopped_exposure_error": exposure_error,
    }
    if breach and alive:
        report["action"] = "stop_runner_cancel_start_maker_flatten"
        report["action_result"] = _stop_runner_cancel_and_flatten()
        ALERT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    elif not alive and stopped_exposure and stopped_exposure.get("has_exposure"):
        if flatten_alive:
            report["action"] = "maker_flatten_running"
        else:
            report["action"] = "stopped_runner_cancel_start_maker_flatten"
            report["action_result"] = _stop_runner_cancel_and_flatten()
            ALERT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    else:
        report["action"] = "none"
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    with HISTORY_PATH.open("a") as f:
        f.write(json.dumps(report, ensure_ascii=False) + "\n")
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
