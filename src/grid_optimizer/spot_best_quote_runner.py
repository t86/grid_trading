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


def spot_best_quote_client_order_prefix(symbol: str) -> str:
    return f"sbq{_symbol_output_slug(symbol)[:5]}"


def is_spot_best_quote_order(order: dict[str, Any], prefix: str) -> bool:
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


def _normalize_order(
    *,
    side: str,
    price: float,
    qty: float,
    tag: str,
    symbol_info: dict[str, Any],
    time_in_force: str = "GTC",
) -> dict[str, Any] | None:
    rounded_price = _round_order_price(price, symbol_info.get("tick_size"), side)
    rounded_qty = _round_order_qty(qty, symbol_info.get("step_size"))
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")
    if rounded_price <= 0 or rounded_qty <= 0:
        return None
    if min_qty is not None and rounded_qty < float(min_qty):
        return None
    if min_notional is not None and rounded_qty * rounded_price < float(min_notional):
        return None
    return {
        "side": side,
        "price": rounded_price,
        "qty": rounded_qty,
        "time_in_force": str(time_in_force or "GTC").upper().strip() or "GTC",
        "tag": tag,
    }


def _spot_order_age_seconds(order: dict[str, Any], now_ms: int | None = None) -> float:
    order_time_ms = max(
        int(_safe_float(order.get("workingTime"))),
        int(_safe_float(order.get("updateTime"))),
        int(_safe_float(order.get("time"))),
    )
    if order_time_ms <= 0:
        return 0.0
    current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    return max((current_ms - order_time_ms) / 1000.0, 0.0)


def apply_quote_sell_timeout_override(
    *,
    desired_orders: list[dict[str, Any]],
    existing_orders: list[dict[str, Any]],
    prefix: str,
    bid_price: float,
    symbol_info: dict[str, Any],
    timeout_seconds: float,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    if timeout_seconds <= 0:
        return [dict(order) for order in desired_orders]
    updated_orders = [dict(order) for order in desired_orders]
    sell_index = next(
        (
            index
            for index, order in enumerate(updated_orders)
            if str(order.get("side", "")).upper() == "SELL"
            and str(order.get("time_in_force", "GTC")).upper() == "GTC"
        ),
        None,
    )
    if sell_index is None:
        return updated_orders
    stale_sell_exists = any(
        is_spot_best_quote_order(order, prefix)
        and str(order.get("side", "")).upper() == "SELL"
        and _spot_order_age_seconds(order, now_ms) >= timeout_seconds
        for order in existing_orders
        if isinstance(order, dict)
    )
    if not stale_sell_exists:
        return updated_orders
    sell_order = updated_orders[sell_index]
    taker_sell = _normalize_order(
        side="SELL",
        price=bid_price,
        qty=_safe_float(sell_order.get("qty", sell_order.get("quantity"))),
        tag="asksell",
        symbol_info=symbol_info,
        time_in_force="IOC",
    )
    if taker_sell is None:
        return updated_orders
    updated_orders[sell_index] = taker_sell
    return updated_orders


def filter_normalized_missing_orders(
    missing_orders: list[dict[str, Any]],
    symbol_info: dict[str, Any],
) -> list[dict[str, Any]]:
    filtered_orders: list[dict[str, Any]] = []
    for order in missing_orders:
        normalized = _normalize_order(
            side=str(order.get("side", "")).upper(),
            price=_safe_float(order.get("price")),
            qty=_safe_float(order.get("qty", order.get("quantity"))),
            tag="diff",
            symbol_info=symbol_info,
            time_in_force=str(order.get("time_in_force", order.get("timeInForce", "GTC"))),
        )
        if normalized is None:
            continue
        filtered_orders.append(
            {
                "side": normalized["side"],
                "price": normalized["price"],
                "qty": normalized["qty"],
                "quantity": normalized["qty"],
                "time_in_force": normalized["time_in_force"],
            }
        )
    return filtered_orders


def preserve_same_price_orders(
    *,
    desired_orders: list[dict[str, Any]],
    existing_orders: list[dict[str, Any]],
    qty_tolerance_ratio: float = 0.01,
    qty_tolerance_abs: float = 5.0,
) -> list[dict[str, Any]]:
    safe_ratio = max(_safe_float(qty_tolerance_ratio), 0.0)
    safe_abs = max(_safe_float(qty_tolerance_abs), 0.0)
    remaining_orders = [dict(order) for order in desired_orders]
    if not remaining_orders or (safe_ratio <= 0 and safe_abs <= 0):
        return remaining_orders

    existing_buckets: dict[tuple[str, float, str], list[dict[str, Any]]] = {}
    for order in existing_orders:
        if not isinstance(order, dict):
            continue
        side = str(order.get("side", "")).upper().strip()
        price = _safe_float(order.get("price"))
        tif = str(order.get("timeInForce", order.get("time_in_force", "GTC"))).upper().strip() or "GTC"
        qty = _safe_float(order.get("origQty", order.get("qty", order.get("quantity"))))
        if side not in {"BUY", "SELL"} or price <= 0 or qty <= 0:
            continue
        key = (side, price, tif)
        existing_buckets.setdefault(key, []).append(order)

    adjusted_orders: list[dict[str, Any]] = []
    for order in remaining_orders:
        side = str(order.get("side", "")).upper().strip()
        price = _safe_float(order.get("price"))
        tif = str(order.get("time_in_force", order.get("timeInForce", "GTC"))).upper().strip() or "GTC"
        qty = _safe_float(order.get("qty", order.get("quantity")))
        if side not in {"BUY", "SELL"} or price <= 0 or qty <= 0:
            adjusted_orders.append(order)
            continue
        key = (side, price, tif)
        bucket = existing_buckets.get(key, [])
        matched_index = None
        matched_existing_qty = None
        for index, existing in enumerate(bucket):
            existing_qty = _safe_float(existing.get("origQty", existing.get("qty", existing.get("quantity"))))
            tolerance = max(safe_abs, existing_qty * safe_ratio, qty * safe_ratio)
            if abs(existing_qty - qty) <= tolerance + 1e-12:
                matched_index = index
                matched_existing_qty = existing_qty
                break
        if matched_index is None:
            adjusted_orders.append(order)
            continue
        bucket.pop(matched_index)
        if not bucket:
            existing_buckets.pop(key, None)
        adjusted_order = dict(order)
        if matched_existing_qty is not None:
            adjusted_order["qty"] = matched_existing_qty
            adjusted_order["quantity"] = matched_existing_qty
        adjusted_orders.append(adjusted_order)
    return adjusted_orders


def build_spot_best_quote_orders_from_snapshot(
    *,
    symbol: str,
    bid_price: float,
    ask_price: float,
    account_info: dict[str, Any],
    symbol_info: dict[str, Any],
    base_position_notional: float,
    quote_buy_order_notional: float,
    quote_sell_order_notional: float,
    max_inventory_multiplier: float,
    min_profit_offset: float,
) -> dict[str, Any]:
    del symbol
    if bid_price <= 0 or ask_price <= 0:
        raise ValueError("bid_price and ask_price must be > 0")

    base_asset = str(symbol_info.get("base_asset") or "").upper().strip()
    quote_asset = str(symbol_info.get("quote_asset") or "").upper().strip()
    if not base_asset or not quote_asset:
        raise ValueError("spot symbol config missing base_asset or quote_asset")

    base_free, base_locked = _extract_spot_balance(account_info, base_asset)
    quote_free, _quote_locked = _extract_spot_balance(account_info, quote_asset)
    inventory_qty = max(base_free + base_locked, 0.0)
    inventory_notional = inventory_qty * bid_price
    base_target = max(float(base_position_notional), 0.0)
    max_inventory_notional = base_target * max(float(max_inventory_multiplier), 1.0)

    if inventory_notional < base_target:
        mode = "bootstrap"
    elif inventory_notional > max_inventory_notional:
        mode = "reduce_only"
    else:
        mode = "quote"

    orders: list[dict[str, Any]] = []
    warnings: list[str] = []

    if mode in {"bootstrap", "quote"} and quote_free > 0:
        buy_notional = min(max(float(quote_buy_order_notional), 0.0), quote_free)
        if mode == "quote":
            remaining_quote_capacity = max(max_inventory_notional - inventory_notional, 0.0)
            buy_notional = min(buy_notional, remaining_quote_capacity)
        buy_order = _normalize_order(
            side="BUY",
            price=bid_price,
            qty=(buy_notional / bid_price) if bid_price > 0 else 0.0,
            tag="bidbuy",
            symbol_info=symbol_info,
        )
        if buy_order:
            orders.append(buy_order)
        else:
            warnings.append("buy order filtered by exchange constraints")

    if mode in {"quote", "reduce_only"} and base_free > 0:
        sell_price = ask_price if mode == "quote" else bid_price
        sell_notional_cap = max(float(quote_sell_order_notional), 0.0)
        sell_qty = min(base_free, sell_notional_cap / max(sell_price, 1e-12))
        if mode == "reduce_only":
            target_excess_qty = max((inventory_notional - max_inventory_notional) / max(sell_price, 1e-12), 0.0)
            sell_qty = min(base_free, max(sell_qty, target_excess_qty))
            sell_time_in_force = "IOC"
        else:
            sell_activation_notional = base_target
            if inventory_notional <= sell_activation_notional:
                sell_qty = 0.0
            else:
                base_reserve_qty = base_target / max(bid_price, 1e-12)
                sellable_qty = max(base_free - base_reserve_qty, 0.0)
                sell_qty = min(sell_qty, sellable_qty)
            sell_time_in_force = "GTC"
        sell_order = _normalize_order(
            side="SELL",
            price=sell_price,
            qty=sell_qty,
            tag="asksell",
            symbol_info=symbol_info,
            time_in_force=sell_time_in_force,
        )
        if sell_order:
            orders.append(sell_order)
        else:
            warnings.append("sell order filtered by exchange constraints")

    return {
        "mode": mode,
        "orders": orders,
        "warnings": warnings,
        "inventory_qty": inventory_qty,
        "inventory_notional": inventory_notional,
        "base_free": base_free,
        "base_locked": base_locked,
        "quote_free": quote_free,
    }


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


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
    prefix = str(args.client_order_prefix or spot_best_quote_client_order_prefix(args.symbol)).strip()
    events_path = Path(args.events_jsonl)
    consecutive_errors = 0
    cycle = 0
    _install_signal_handlers()

    while True:
        cycle += 1
        cycle_ts = datetime.now(timezone.utc).isoformat()
        try:
            book = fetch_spot_book_tickers(symbol=args.symbol)
            if not book:
                raise RuntimeError(f"{args.symbol} missing spot book")
            bid_price = _safe_float(book[0].get("bid_price"))
            ask_price = _safe_float(book[0].get("ask_price"))
            account_info = fetch_spot_account_info(api_key, api_secret, recv_window=args.recv_window)
            symbol_info = fetch_spot_symbol_config(args.symbol)
            snapshot = build_spot_best_quote_orders_from_snapshot(
                symbol=args.symbol,
                bid_price=bid_price,
                ask_price=ask_price,
                account_info=account_info,
                symbol_info=symbol_info,
                base_position_notional=args.base_position_notional,
                quote_buy_order_notional=args.quote_buy_order_notional,
                quote_sell_order_notional=args.quote_sell_order_notional,
                max_inventory_multiplier=args.max_inventory_multiplier,
                min_profit_offset=args.min_profit_offset,
            )
            open_orders = fetch_spot_open_orders(args.symbol, api_key, api_secret, recv_window=args.recv_window)
            our_orders = [order for order in open_orders if isinstance(order, dict) and is_spot_best_quote_order(order, prefix)]
            desired_orders = [
                {
                    "side": item["side"],
                    "price": item["price"],
                    "quantity": item["qty"],
                    "qty": item["qty"],
                    "time_in_force": item["time_in_force"],
                }
                for item in list(snapshot.get("orders", []))
            ]
            desired_orders = apply_quote_sell_timeout_override(
                desired_orders=desired_orders,
                existing_orders=our_orders,
                prefix=prefix,
                bid_price=bid_price,
                symbol_info=symbol_info,
                timeout_seconds=max(float(args.reduce_only_timeout_seconds), 0.0),
            )
            desired_orders = preserve_same_price_orders(
                desired_orders=desired_orders,
                existing_orders=our_orders,
            )
            if STOP_REQUESTED:
                cancel_result = _cancel_orders(
                    symbol=args.symbol,
                    api_key=api_key,
                    api_secret=api_secret,
                    orders=our_orders,
                    recv_window=args.recv_window,
                )
                _append_jsonl(events_path, {"ts": cycle_ts, "cycle": cycle, "event": "stop_requested", "cancel_result": cancel_result})
                return 0

            diff = diff_open_orders(existing_orders=our_orders, desired_orders=desired_orders)
            diff["missing_orders"] = filter_normalized_missing_orders(
                list(diff.get("missing_orders", [])),
                symbol_info,
            )
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
                client_order_id = _build_client_order_id(prefix, str(order["side"]).lower())
                try:
                    response = post_spot_order(
                        symbol=args.symbol,
                        side=str(order["side"]),
                        quantity=float(order["qty"]),
                        price=float(order["price"]),
                        api_key=api_key,
                        api_secret=api_secret,
                        order_type="LIMIT",
                        time_in_force=str(order.get("time_in_force") or "GTC"),
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
                    "event": "best_quote_cycle",
                    "symbol": args.symbol,
                    "bid_price": bid_price,
                    "ask_price": ask_price,
                    "mode": snapshot.get("mode"),
                    "inventory_qty": snapshot.get("inventory_qty"),
                    "inventory_notional": snapshot.get("inventory_notional"),
                    "quote_free": snapshot.get("quote_free"),
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
                    f"Spot best-quote runner stopped after {consecutive_errors} consecutive errors"
                ) from exc
        time.sleep(max(float(args.sleep_seconds), 0.5))


def main() -> None:
    parser = argparse.ArgumentParser(description="Conservative top-of-book spot quote runner.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--client-order-prefix", type=str, default="")
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--max-consecutive-errors", type=int, default=20)
    parser.add_argument("--events-jsonl", type=str, required=True)
    parser.add_argument("--base-position-notional", type=float, default=50.0)
    parser.add_argument("--quote-buy-order-notional", type=float, default=10.0)
    parser.add_argument("--quote-sell-order-notional", type=float, default=12.0)
    parser.add_argument("--max-inventory-multiplier", type=float, default=1.6)
    parser.add_argument("--min-profit-offset", type=float, default=0.0001)
    parser.add_argument("--reduce-only-timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    raise SystemExit(_run(args))


if __name__ == "__main__":
    main()
