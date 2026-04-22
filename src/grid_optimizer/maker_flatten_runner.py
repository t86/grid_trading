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
    delete_futures_order,
    fetch_futures_account_info_v3,
    fetch_futures_book_tickers,
    fetch_futures_open_orders,
    fetch_futures_position_mode,
    fetch_futures_symbol_config,
    load_binance_api_credentials,
    post_futures_order,
)
from .dry_run import _round_order_price, _round_order_qty
from .live_check import extract_symbol_position
from .semi_auto_plan import diff_open_orders

STOP_REQUESTED = False


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _symbol_output_slug(symbol: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(symbol or "").strip()).strip("_").lower()
    return normalized or "symbol"


def flatten_client_order_prefix(symbol: str) -> str:
    return f"mf{_symbol_output_slug(symbol)[:6]}"


def is_flatten_order(order: dict[str, Any], prefix: str) -> bool:
    client_order_id = str(order.get("clientOrderId", "") or "")
    return client_order_id.startswith(prefix)


def _build_client_order_id(prefix: str, tag: str, side: str) -> str:
    side_text = "b" if str(side).upper().strip() == "BUY" else "s"
    nonce = int(time.time() * 1000) % 1_000_000_000
    return f"{prefix}_{tag[:8]}_{side_text}_{nonce}"


def _position_cost_basis_price(position: dict[str, Any]) -> float:
    for key in ("breakEvenPrice", "entryPrice"):
        price = max(_safe_float(position.get(key)), 0.0)
        if price > 0:
            return price
    return 0.0


def build_flatten_orders_from_snapshot(
    *,
    symbol: str,
    bid_price: float,
    ask_price: float,
    dual_side_position: bool,
    account_info: dict[str, Any],
    symbol_info: dict[str, Any],
    allow_loss: bool = False,
    min_profit_ratio: float = 0.0,
) -> dict[str, Any]:
    if bid_price <= 0 or ask_price <= 0:
        raise ValueError("bid_price and ask_price must be > 0")

    safe_min_profit_ratio = max(_safe_float(min_profit_ratio), 0.0)
    result = {
        "orders": [],
        "warnings": [],
        "dual_side_position": bool(dual_side_position),
        "allow_loss": bool(allow_loss),
        "min_profit_ratio": safe_min_profit_ratio,
        "long_qty": 0.0,
        "short_qty": 0.0,
        "net_qty": 0.0,
    }
    tick_size = symbol_info.get("tick_size")
    step_size = symbol_info.get("step_size")
    min_qty = symbol_info.get("min_qty")
    min_notional = symbol_info.get("min_notional")

    def _append_order(
        *,
        qty: float,
        side: str,
        price: float,
        reduce_only: bool | None,
        position_side: str | None,
        tag: str,
        cost_basis_price: float,
    ) -> None:
        rounded_price = _round_order_price(price, tick_size, side)
        rounded_qty = _round_order_qty(abs(qty), step_size)
        if rounded_qty <= 0:
            result["warnings"].append(f"{symbol} {position_side or 'BOTH'} 平仓数量向下取整后为 0，已跳过")
            return
        if min_qty is not None and rounded_qty < float(min_qty):
            result["warnings"].append(f"{symbol} {position_side or 'BOTH'} 平仓数量低于最小数量，已跳过")
            return
        if min_notional is not None and rounded_qty * rounded_price < float(min_notional):
            result["warnings"].append(f"{symbol} {position_side or 'BOTH'} 平仓名义低于最小下单额，已跳过")
            return
        if not allow_loss:
            safe_cost = max(_safe_float(cost_basis_price), 0.0)
            side_text = str(side).upper().strip()
            side_label = position_side or "BOTH"
            if safe_cost <= 0:
                result["warnings"].append(f"{symbol} {side_label} 缺少成本基准，默认禁止 maker_flatten 平仓")
                return
            if side_text == "SELL":
                floor_price = _round_order_price(safe_cost * (1.0 + safe_min_profit_ratio), tick_size, "SELL")
                if rounded_price + 1e-12 < floor_price:
                    result["warnings"].append(
                        f"{symbol} {side_label} maker_flatten 价格 {rounded_price} 低于成本保护价 {floor_price}，默认禁止亏损平仓"
                    )
                    return
            elif side_text == "BUY":
                ceiling_price = _round_order_price(safe_cost * (1.0 - safe_min_profit_ratio), tick_size, "BUY")
                if ceiling_price <= 0 or rounded_price > ceiling_price + 1e-12:
                    result["warnings"].append(
                        f"{symbol} {side_label} maker_flatten 价格 {rounded_price} 高于成本保护价 {ceiling_price}，默认禁止亏损平仓"
                    )
                    return
        order = {
            "symbol": symbol,
            "side": side,
            "quantity": rounded_qty,
            "price": rounded_price,
            "time_in_force": "GTX",
            "tag": tag,
        }
        if reduce_only is not None:
            order["reduce_only"] = reduce_only
        if position_side:
            order["position_side"] = position_side
        result["orders"].append(order)

    if dual_side_position:
        long_position = extract_symbol_position(account_info, symbol, "LONG")
        short_position = extract_symbol_position(account_info, symbol, "SHORT")
        long_qty = abs(_safe_float(long_position.get("positionAmt")))
        short_qty = abs(_safe_float(short_position.get("positionAmt")))
        result["long_qty"] = long_qty
        result["short_qty"] = short_qty
        result["net_qty"] = long_qty - short_qty
        if long_qty > 0:
            _append_order(
                qty=long_qty,
                side="SELL",
                price=ask_price,
                reduce_only=None,
                position_side="LONG",
                tag="closelong",
                cost_basis_price=_position_cost_basis_price(long_position),
            )
        if short_qty > 0:
            _append_order(
                qty=short_qty,
                side="BUY",
                price=bid_price,
                reduce_only=None,
                position_side="SHORT",
                tag="closeshort",
                cost_basis_price=_position_cost_basis_price(short_position),
            )
    else:
        position = extract_symbol_position(account_info, symbol)
        position_amt = _safe_float(position.get("positionAmt"))
        result["net_qty"] = position_amt
        result["long_qty"] = max(position_amt, 0.0)
        result["short_qty"] = max(-position_amt, 0.0)
        if position_amt > 0:
            _append_order(
                qty=position_amt,
                side="SELL",
                price=ask_price,
                reduce_only=True,
                position_side=None,
                tag="closelong",
                cost_basis_price=_position_cost_basis_price(position),
            )
        elif position_amt < 0:
            _append_order(
                qty=abs(position_amt),
                side="BUY",
                price=bid_price,
                reduce_only=True,
                position_side=None,
                tag="closeshort",
                cost_basis_price=_position_cost_basis_price(position),
            )

    return result


def _normalize_position_side_filter(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text in {"LONG", "CLOSELONG", "CLOSE_LONG"}:
        return "LONG"
    if text in {"SHORT", "CLOSESHORT", "CLOSE_SHORT"}:
        return "SHORT"
    return None


def filter_flatten_orders_for_position_side(
    orders: list[dict[str, Any]],
    position_side_filter: Any,
) -> list[dict[str, Any]]:
    normalized_filter = _normalize_position_side_filter(position_side_filter)
    if normalized_filter is None:
        return list(orders)
    expected_tag = "closelong" if normalized_filter == "LONG" else "closeshort"
    expected_side = "SELL" if normalized_filter == "LONG" else "BUY"
    return [
        order
        for order in orders
        if str(order.get("tag", "")).strip().lower() == expected_tag
        or str(order.get("position_side", "")).strip().upper() == normalized_filter
        or (
            str(order.get("position_side", "")).strip().upper() in {"", "BOTH"}
            and str(order.get("side", "")).strip().upper() == expected_side
        )
    ]


def load_live_flatten_snapshot(
    symbol: str,
    api_key: str,
    api_secret: str,
    *,
    allow_loss: bool = False,
    min_profit_ratio: float = 0.0,
) -> dict[str, Any]:
    symbol_info = fetch_futures_symbol_config(symbol)
    book = fetch_futures_book_tickers(symbol=symbol)
    if not book:
        raise RuntimeError(f"{symbol} 缺少盘口数据，无法按买一/卖一挂 maker 平仓")
    bid_price = _safe_float(book[0].get("bid_price"))
    ask_price = _safe_float(book[0].get("ask_price"))
    if bid_price <= 0 or ask_price <= 0:
        raise RuntimeError(f"{symbol} 盘口价格无效，无法按买一/卖一挂 maker 平仓")
    account_info = fetch_futures_account_info_v3(api_key, api_secret)
    position_mode = fetch_futures_position_mode(api_key, api_secret)
    dual_side_position = _truthy(position_mode.get("dualSidePosition"))
    snapshot = build_flatten_orders_from_snapshot(
        symbol=symbol,
        bid_price=bid_price,
        ask_price=ask_price,
        dual_side_position=dual_side_position,
        account_info=account_info,
        symbol_info=symbol_info,
        allow_loss=allow_loss,
        min_profit_ratio=min_profit_ratio,
    )
    snapshot["bid_price"] = bid_price
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
    open_orders = fetch_futures_open_orders(symbol, api_key, api_secret, recv_window=recv_window)
    ours = [order for order in open_orders if isinstance(order, dict) and is_flatten_order(order, prefix)]
    result = {"attempted": len(ours), "canceled": 0, "errors": []}
    for order in ours:
        try:
            delete_futures_order(
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
            delete_futures_order(
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
    prefix = str(args.client_order_prefix or flatten_client_order_prefix(args.symbol)).strip()
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
            snapshot = load_live_flatten_snapshot(
                args.symbol,
                api_key,
                api_secret,
                allow_loss=bool(args.allow_loss),
                min_profit_ratio=float(args.min_profit_ratio),
            )
            open_orders = fetch_futures_open_orders(args.symbol, api_key, api_secret, recv_window=args.recv_window)
            our_orders = [order for order in open_orders if isinstance(order, dict) and is_flatten_order(order, prefix)]
            raw_orders = list(snapshot.get("orders", []))
            orders = filter_flatten_orders_for_position_side(raw_orders, args.position_side_filter)
            if args.position_side_filter:
                snapshot = {
                    **snapshot,
                    "orders": orders,
                    "unfiltered_order_count": len(raw_orders),
                    "position_side_filter": _normalize_position_side_filter(args.position_side_filter),
                }
            desired_orders = [
                {
                    **order,
                    "side": str(order["side"]),
                    "price": _safe_float(order["price"]),
                    "qty": _safe_float(order["quantity"]),
                    "position_side": order.get("position_side", "BOTH"),
                }
                for order in orders
            ]
            diff = diff_open_orders(existing_orders=our_orders, desired_orders=desired_orders)
            cancel_result = _cancel_orders(
                symbol=args.symbol,
                api_key=api_key,
                api_secret=api_secret,
                orders=list(diff.get("stale_orders", [])),
                recv_window=args.recv_window,
            )
            placed_orders: list[dict[str, Any]] = []
            place_errors: list[dict[str, Any]] = []
            if not orders:
                _append_jsonl(
                    events_path,
                    {
                        "ts": cycle_ts,
                        "cycle": cycle,
                        "event": "flat",
                        "symbol": args.symbol,
                        "snapshot": snapshot,
                        "cancel_result": cancel_result,
                        "kept_order_count": len(diff.get("kept_orders", [])),
                    },
                )
                return 0
            for order in diff.get("missing_orders", []):
                client_order_id = _build_client_order_id(prefix, str(order.get("tag", "close")), str(order.get("side", "")))
                try:
                    response = post_futures_order(
                        symbol=args.symbol,
                        side=str(order["side"]),
                        quantity=_safe_float(order["quantity"]),
                        price=_safe_float(order["price"]),
                        api_key=api_key,
                        api_secret=api_secret,
                        time_in_force=str(order.get("time_in_force", "GTX")),
                        recv_window=args.recv_window,
                        new_client_order_id=client_order_id,
                        reduce_only=order.get("reduce_only"),
                        position_side=order.get("position_side"),
                    )
                    placed_orders.append(
                        {
                            **order,
                            "client_order_id": client_order_id,
                            "order_id": response.get("orderId"),
                        }
                    )
                except Exception as exc:
                    place_errors.append(
                        {
                            **order,
                            "client_order_id": client_order_id,
                            "message": f"{type(exc).__name__}: {exc}",
                        }
                    )
            _append_jsonl(
                events_path,
                {
                    "ts": cycle_ts,
                    "cycle": cycle,
                    "event": "cycle",
                    "symbol": args.symbol,
                    "snapshot": snapshot,
                    "kept_order_count": len(diff.get("kept_orders", [])),
                    "stale_order_count": len(diff.get("stale_orders", [])),
                    "missing_order_count": len(diff.get("missing_orders", [])),
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
                raise RuntimeError(f"too many consecutive errors: {consecutive_errors}") from exc
        time.sleep(args.sleep_seconds)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Maker-only top-of-book flatten runner for Binance futures.")
    parser.add_argument("--symbol", type=str, required=True)
    parser.add_argument("--client-order-prefix", type=str, default="")
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--max-consecutive-errors", type=int, default=20)
    parser.add_argument("--events-jsonl", type=str, required=True)
    parser.add_argument("--position-side-filter", type=str, default="")
    parser.add_argument("--allow-loss", action="store_true")
    parser.add_argument("--min-profit-ratio", type=float, default=0.0)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    args.symbol = str(args.symbol).upper().strip()
    if not args.symbol:
        raise SystemExit("--symbol is required")
    if args.sleep_seconds <= 0:
        raise SystemExit("--sleep-seconds must be > 0")
    if args.max_consecutive_errors <= 0:
        raise SystemExit("--max-consecutive-errors must be > 0")
    if args.min_profit_ratio < 0:
        raise SystemExit("--min-profit-ratio must be >= 0")
    raise SystemExit(_run(args))


if __name__ == "__main__":
    main()
