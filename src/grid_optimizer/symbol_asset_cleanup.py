from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Callable

from .data import (
    delete_futures_order,
    delete_spot_order,
    fetch_futures_open_orders,
    fetch_futures_position_risk_v3,
    fetch_futures_symbol_config,
    fetch_spot_account_info,
    fetch_spot_book_tickers,
    fetch_spot_open_orders,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
    post_futures_market_order,
    post_spot_order,
)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def round_down(value: float, step: float) -> float:
    if step <= 0:
        return max(float(value), 0.0)
    increment = Decimal(str(step))
    units = (Decimal(str(max(value, 0.0))) / increment).to_integral_value(rounding=ROUND_DOWN)
    return float(units * increment)


def build_futures_close_requests(
    *, positions: list[dict[str, Any]], symbol: str, step_size: float
) -> list[dict[str, Any]]:
    normalized_symbol = symbol.upper().strip()
    requests: list[dict[str, Any]] = []
    for position in positions:
        if str(position.get("symbol", "")).upper().strip() != normalized_symbol:
            continue
        amount = _safe_float(position.get("positionAmt"))
        quantity = round_down(abs(amount), step_size)
        if quantity <= 0:
            continue
        position_side = str(position.get("positionSide") or "BOTH").upper().strip() or "BOTH"
        side = "SELL" if amount > 0 else "BUY"
        requests.append(
            {
                "side": side,
                "quantity": quantity,
                "position_side": position_side if position_side in {"LONG", "SHORT"} else None,
                "reduce_only": None if position_side in {"LONG", "SHORT"} else True,
            }
        )
    return requests


def _cancel_all(
    orders: list[dict[str, Any]], cancel: Callable[[int | None, str | None], Any]
) -> dict[str, Any]:
    result: dict[str, Any] = {"attempted": len(orders), "success": 0, "errors": []}
    for order in orders:
        order_id = int(order["orderId"]) if order.get("orderId") is not None else None
        client_id = str(order.get("clientOrderId") or "").strip() or None
        try:
            cancel(order_id, client_id)
            result["success"] += 1
        except Exception as exc:
            result["errors"].append(
                {
                    "order_id": order_id,
                    "client_order_id": client_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    return result


def _balance(account: dict[str, Any], asset: str) -> dict[str, float]:
    for item in account.get("balances", []):
        if isinstance(item, dict) and str(item.get("asset", "")).upper() == asset.upper():
            return {"free": _safe_float(item.get("free")), "locked": _safe_float(item.get("locked"))}
    return {"free": 0.0, "locked": 0.0}


def _snapshot(symbol: str, api_key: str, api_secret: str) -> dict[str, Any]:
    spot_config = fetch_spot_symbol_config(symbol)
    futures_config = fetch_futures_symbol_config(symbol)
    base_asset = str(spot_config.get("base_asset") or symbol.removesuffix("USDT")).upper()
    account = fetch_spot_account_info(api_key, api_secret)
    positions = fetch_futures_position_risk_v3(api_key, api_secret, symbol=symbol)
    return {
        "base_asset": base_asset,
        "spot_config": spot_config,
        "futures_config": futures_config,
        "spot_balance": _balance(account, base_asset),
        "spot_open_orders": fetch_spot_open_orders(symbol, api_key, api_secret),
        "futures_open_orders": fetch_futures_open_orders(symbol, api_key, api_secret, use_cache=False),
        "futures_positions": positions,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Cancel and liquidate one Binance spot/futures symbol.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--settle-wait-seconds", type=float, default=1.5)
    parser.add_argument("--status-json", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    symbol = str(args.symbol).upper().strip()
    creds = load_binance_api_credentials()
    if not creds:
        raise SystemExit("Binance API credentials required")
    api_key, api_secret = creds
    status_path = Path(args.status_json) if str(args.status_json).strip() else Path("output") / f"{symbol.lower()}_asset_cleanup_status.json"

    before = _snapshot(symbol, api_key, api_secret)
    result: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "apply": bool(args.apply),
        "before": before,
        "spot_cancel": None,
        "futures_cancel": None,
        "futures_close_orders": [],
        "spot_sell_order": None,
        "errors": [],
    }
    if not args.apply:
        result["planned_futures_close_requests"] = build_futures_close_requests(
            positions=before["futures_positions"],
            symbol=symbol,
            step_size=_safe_float(before["futures_config"].get("step_size")),
        )
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
        return 0

    result["spot_cancel"] = _cancel_all(
        before["spot_open_orders"],
        lambda order_id, client_id: delete_spot_order(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            order_id=order_id,
            orig_client_order_id=client_id,
        ),
    )
    result["futures_cancel"] = _cancel_all(
        before["futures_open_orders"],
        lambda order_id, client_id: delete_futures_order(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            order_id=order_id,
            orig_client_order_id=client_id,
        ),
    )

    close_requests = build_futures_close_requests(
        positions=before["futures_positions"],
        symbol=symbol,
        step_size=_safe_float(before["futures_config"].get("step_size")),
    )
    for index, request in enumerate(close_requests, start=1):
        try:
            response = post_futures_market_order(
                symbol=symbol,
                side=request["side"],
                quantity=request["quantity"],
                api_key=api_key,
                api_secret=api_secret,
                reduce_only=request["reduce_only"],
                position_side=request["position_side"],
                new_client_order_id=f"cleanup-{symbol.lower()[:8]}-f-{index}-{int(time.time()) % 1000000}",
            )
            result["futures_close_orders"].append(response)
        except Exception as exc:
            result["errors"].append({"stage": "futures_close", "request": request, "error": f"{type(exc).__name__}: {exc}"})

    account = fetch_spot_account_info(api_key, api_secret)
    base_asset = before["base_asset"]
    base_balance = _balance(account, base_asset)
    spot_step = _safe_float(before["spot_config"].get("step_size"))
    spot_min_qty = _safe_float(before["spot_config"].get("min_qty"))
    spot_min_notional = _safe_float(before["spot_config"].get("min_notional"))
    sell_qty = round_down(base_balance["free"], spot_step)
    book_rows = fetch_spot_book_tickers(symbol=symbol)
    bid_price = _safe_float(book_rows[0].get("bid_price")) if book_rows else 0.0
    if sell_qty >= spot_min_qty and sell_qty * bid_price >= spot_min_notional:
        try:
            result["spot_sell_order"] = post_spot_order(
                symbol=symbol,
                side="SELL",
                quantity=sell_qty,
                price=0.0,
                api_key=api_key,
                api_secret=api_secret,
                order_type="MARKET",
                new_client_order_id=f"cleanup-{symbol.lower()[:8]}-s-{int(time.time()) % 1000000}",
            )
        except Exception as exc:
            result["errors"].append({"stage": "spot_sell", "quantity": sell_qty, "error": f"{type(exc).__name__}: {exc}"})

    time.sleep(max(float(args.settle_wait_seconds), 0.0))
    after = _snapshot(symbol, api_key, api_secret)
    result["after"] = after
    futures_step = max(_safe_float(after["futures_config"].get("step_size")), 1e-12)
    positions_flat = all(
        abs(_safe_float(item.get("positionAmt"))) < futures_step
        for item in after["futures_positions"]
        if str(item.get("symbol", "")).upper().strip() == symbol
    )
    balance = after["spot_balance"]
    spot_dust = balance["free"] < max(spot_step, 1e-12) or balance["free"] * bid_price < spot_min_notional
    result["verification"] = {
        "spot_open_orders_zero": len(after["spot_open_orders"]) == 0,
        "futures_open_orders_zero": len(after["futures_open_orders"]) == 0,
        "futures_positions_flat": positions_flat,
        "spot_locked_zero": balance["locked"] <= 1e-12,
        "spot_free_is_dust": spot_dust,
    }
    result["ok"] = not result["errors"] and all(result["verification"].values())
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
