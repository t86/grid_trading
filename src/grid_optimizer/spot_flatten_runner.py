from __future__ import annotations

import argparse
import json
import re
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .data import (
    delete_spot_order,
    fetch_spot_account_info,
    fetch_spot_book_tickers,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
    post_spot_order,
)
from .dry_run import _round_order_price, _round_order_qty
from .semi_auto_plan import diff_open_orders

STOP_REQUESTED = False


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _symbol_output_slug(symbol: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(symbol or "").strip()).strip("_").lower()
    return normalized or "symbol"


def spot_flatten_client_order_prefix(symbol: str) -> str:
    return f"sf{_symbol_output_slug(symbol)[:6]}"


def is_spot_flatten_order(order: dict[str, Any], prefix: str) -> bool:
    client_order_id = str(order.get("clientOrderId", "") or "")
    return client_order_id.startswith(prefix)


def _build_client_order_id(prefix: str, tag: str) -> str:
    nonce = int(time.time() * 1000) % 1_000_000_000
    return f"{prefix}_{tag[:12]}_{nonce}"


def _extract_spot_balance(account_info: dict[str, Any], asset: str) -> tuple[float, float]:
    wanted = str(asset or "").upper().strip()
    balances = account_info.get("balances", [])
    if not isinstance(balances, list):
        return 0.0, 0.0
    for item in balances:
        if not isinstance(item, dict):
            continue
        if str(item.get("asset", "")).upper().strip() != wanted:
            continue
        return _safe_float(item.get("free")), _safe_float(item.get("locked"))
    return 0.0, 0.0


def build_spot_flatten_orders_from_snapshot(
    *,
    symbol: str,
    ask_price: float,
    account_info: dict[str, Any],
    symbol_info: dict[str, Any],
) -> dict[str, Any]:
    if ask_price <= 0:
        raise ValueError("ask_price must be > 0")

    base_asset = str(symbol_info.get("base_asset") or "").upper().strip()
    if not base_asset:
        raise ValueError("spot symbol config missing base_asset")

    free_qty, locked_qty = _extract_spot_balance(account_info, base_asset)
    inventory_qty = max(free_qty + locked_qty, 0.0)
    tick_size = symbol_info.get("tick_size")
    step_size = symbol_info.get("step_size")
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")

    result = {
        "orders": [],
        "warnings": [],
        "inventory_qty": inventory_qty,
        "base_asset": base_asset,
    }
    if inventory_qty <= 0:
        return result

    rounded_price = _round_order_price(ask_price, tick_size, "SELL")
    rounded_qty = _round_order_qty(inventory_qty, step_size)
    if rounded_qty <= 0:
        result["warnings"].append(f"{symbol} flatten quantity rounds to 0")
        return result
    if min_qty is not None and rounded_qty < float(min_qty):
        result["warnings"].append(f"{symbol} flatten quantity below min_qty")
        return result
    if min_notional is not None and rounded_qty * rounded_price < float(min_notional):
        result["warnings"].append(f"{symbol} flatten notional below min_notional")
        return result
    result["orders"].append(
        {
            "symbol": symbol,
            "side": "SELL",
            "quantity": rounded_qty,
            "price": rounded_price,
            "time_in_force": "GTX",
            "tag": "flatten",
        }
    )
    return result


def load_live_spot_flatten_snapshot(symbol: str, api_key: str, api_secret: str) -> dict[str, Any]:
    symbol_info = fetch_spot_symbol_config(symbol)
    book = fetch_spot_book_tickers(symbol=symbol)
    if not book:
        raise RuntimeError(f"{symbol} 缺少盘口数据，无法按卖一挂 maker 平仓")
    ask_price = _safe_float(book[0].get("ask_price"))
    if ask_price <= 0:
        raise RuntimeError(f"{symbol} 卖一价格无效，无法按卖一挂 maker 平仓")
    account_info = fetch_spot_account_info(api_key, api_secret)
    snapshot = build_spot_flatten_orders_from_snapshot(
        symbol=symbol,
        ask_price=ask_price,
        account_info=account_info,
        symbol_info=symbol_info,
    )
    snapshot["ask_price"] = ask_price
    snapshot["symbol_info"] = symbol_info
    return snapshot


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _cancel_our_orders(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    prefix: str,
    recv_window: int,
) -> dict[str, Any]:
    open_orders = fetch_spot_open_orders(symbol, api_key, api_secret, recv_window=recv_window)
    ours = [order for order in open_orders if isinstance(order, dict) and is_spot_flatten_order(order, prefix)]
    result = {"attempted": len(ours), "canceled": 0, "errors": []}
    for order in ours:
        try:
            delete_spot_order(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                order_id=int(order["orderId"]) if str(order.get("orderId", "")).strip() else None,
                orig_client_order_id=str(order.get("clientOrderId", "")).strip() or None,
                recv_window=recv_window,
            )
            result["canceled"] += 1
        except Exception as exc:
            result["errors"].append(
                {
                    "order_id": order.get("orderId"),
                    "client_order_id": order.get("clientOrderId"),
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
    return result


def _cancel_orders(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    orders: list[dict[str, Any]],
    recv_window: int,
) -> dict[str, Any]:
    result = {"attempted": len(orders), "canceled": 0, "errors": []}
    for order in orders:
        try:
            delete_spot_order(
                symbol=symbol,
                api_key=api_key,
                api_secret=api_secret,
                order_id=int(order["orderId"]) if str(order.get("orderId", "")).strip() else None,
                orig_client_order_id=str(order.get("clientOrderId", "")).strip() or None,
                recv_window=recv_window,
            )
            result["canceled"] += 1
        except Exception as exc:
            result["errors"].append(
                {
                    "order_id": order.get("orderId"),
                    "client_order_id": order.get("clientOrderId"),
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
    return result


def _install_signal_handlers() -> None:
    def _handle_signal(signum: int, _frame: Any) -> None:
        del signum
        global STOP_REQUESTED
        STOP_REQUESTED = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)


def _run(args: argparse.Namespace) -> int:
    creds = load_binance_api_credentials()
    if creds is None:
        raise RuntimeError("missing Binance API credentials")
    api_key, api_secret = creds
    prefix = str(args.client_order_prefix or spot_flatten_client_order_prefix(args.symbol)).strip()
    events_path = Path(args.events_jsonl)
    consecutive_errors = 0
    cycle = 0
    _install_signal_handlers()

    while True:
        cycle += 1
        cycle_ts = datetime.now(timezone.utc).isoformat()
        if STOP_REQUESTED:
            cancel_result = _cancel_our_orders(
                symbol=args.symbol,
                api_key=api_key,
                api_secret=api_secret,
                prefix=prefix,
                recv_window=args.recv_window,
            )
            _append_jsonl(
                events_path,
                {
                    "ts": cycle_ts,
                    "cycle": cycle,
                    "event": "stop_requested",
                    "symbol": args.symbol,
                    "cancel_result": cancel_result,
                },
            )
            return 0
        try:
            snapshot = load_live_spot_flatten_snapshot(args.symbol, api_key, api_secret)
            open_orders = fetch_spot_open_orders(args.symbol, api_key, api_secret, recv_window=args.recv_window)
            our_orders = [order for order in open_orders if isinstance(order, dict) and is_spot_flatten_order(order, prefix)]
            desired_orders = [
                {
                    "side": item["side"],
                    "price": item["price"],
                    "quantity": item["quantity"],
                    "qty": item["quantity"],
                    "time_in_force": item["time_in_force"],
                }
                for item in list(snapshot.get("orders", []))
            ]
            if not desired_orders:
                cancel_result = _cancel_orders(
                    symbol=args.symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    orders=our_orders,
                    recv_window=args.recv_window,
                )
                _append_jsonl(
                    events_path,
                    {
                        "ts": cycle_ts,
                        "cycle": cycle,
                        "event": "flatten_complete",
                        "symbol": args.symbol,
                        "inventory_qty": snapshot.get("inventory_qty", 0.0),
                        "warnings": snapshot.get("warnings", []),
                        "cancel_result": cancel_result,
                    },
                )
                return 0

            diff = diff_open_orders(existing_orders=our_orders, desired_orders=desired_orders)
            cancel_result = _cancel_orders(
                symbol=args.symbol,
                api_key=api_key,
                api_secret=api_secret,
                orders=diff["stale_orders"],
                recv_window=args.recv_window,
            )
            placed_orders: list[dict[str, Any]] = []
            place_errors: list[dict[str, Any]] = []
            for order in diff["missing_orders"]:
                client_order_id = _build_client_order_id(prefix, "flatten")
                try:
                    response = post_spot_order(
                        symbol=args.symbol,
                        side=str(order["side"]),
                        quantity=float(order["qty"]),
                        price=float(order["price"]),
                        api_key=api_key,
                        api_secret=api_secret,
                        order_type="LIMIT_MAKER",
                        recv_window=args.recv_window,
                        new_client_order_id=client_order_id,
                    )
                    placed_orders.append(
                        {
                            "side": order["side"],
                            "price": order["price"],
                            "qty": order["qty"],
                            "order_id": response.get("orderId"),
                            "client_order_id": response.get("clientOrderId"),
                        }
                    )
                except Exception as exc:
                    place_errors.append(
                        {
                            "side": order["side"],
                            "price": order["price"],
                            "qty": order["qty"],
                            "message": f"{type(exc).__name__}: {exc}",
                        }
                    )
            _append_jsonl(
                events_path,
                {
                    "ts": cycle_ts,
                    "cycle": cycle,
                    "event": "flatten_cycle",
                    "symbol": args.symbol,
                    "inventory_qty": snapshot.get("inventory_qty", 0.0),
                    "ask_price": snapshot.get("ask_price"),
                    "warnings": snapshot.get("warnings", []),
                    "cancel_result": cancel_result,
                    "placed_orders": placed_orders,
                    "place_errors": place_errors,
                },
            )
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            _append_jsonl(
                events_path,
                {
                    "ts": cycle_ts,
                    "cycle": cycle,
                    "event": "error",
                    "symbol": args.symbol,
                    "message": f"{type(exc).__name__}: {exc}",
                    "consecutive_errors": consecutive_errors,
                },
            )
            if consecutive_errors >= args.max_consecutive_errors:
                raise RuntimeError(
                    f"Spot flatten runner stopped after {consecutive_errors} consecutive errors"
                ) from exc
        time.sleep(max(float(args.sleep_seconds), 0.5))


def main() -> None:
    parser = argparse.ArgumentParser(description="Maker-only top-of-book flatten runner for Binance spot.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--client-order-prefix", type=str, default="")
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--max-consecutive-errors", type=int, default=20)
    parser.add_argument("--events-jsonl", type=str, required=True)
    args = parser.parse_args()
    raise SystemExit(_run(args))


if __name__ == "__main__":
    main()
