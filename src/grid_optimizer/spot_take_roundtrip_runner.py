from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Callable

from .data import (
    fetch_spot_account_info,
    fetch_spot_book_tickers,
    fetch_spot_symbol_config,
    load_binance_api_credentials,
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


def next_leg_notional(*, remaining_volume: float, per_leg_notional: float, min_notional: float) -> float:
    desired = min(max(per_leg_notional, 0.0), max(remaining_volume, 0.0) / 2.0)
    return max(desired, max(min_notional, 0.0))


def minimum_executable_leg_notional(*, min_notional: float, ask_price: float, step_size: float) -> float:
    return max(min_notional, 0.0) + max(ask_price, 0.0) * max(step_size, 0.0)


def execute_take_roundtrip(
    *,
    leg_notional: float,
    bid_price: float,
    ask_price: float,
    step_size: float,
    min_qty: float,
    min_notional: float,
    submit_market: Callable[[str, float], dict[str, Any]],
) -> dict[str, Any]:
    qty = round_down(max(leg_notional, 0.0) / max(ask_price, 1e-12), step_size)
    if qty <= 0 or qty + 1e-12 < max(min_qty, 0.0) or qty * ask_price + 1e-12 < max(min_notional, 0.0):
        return {"ok": False, "reason": "buy_below_exchange_min", "gross_notional": 0.0}

    buy = submit_market("BUY", qty)
    bought_qty = round_down(_safe_float(buy.get("executedQty")), step_size)
    buy_quote = _safe_float(buy.get("cummulativeQuoteQty"))
    if buy_quote <= 0:
        buy_quote = bought_qty * ask_price
    if bought_qty <= 0:
        return {
            "ok": False,
            "reason": "buy_not_filled",
            "gross_notional": round(buy_quote, 12),
            "buy_order": buy,
        }

    sell = submit_market("SELL", bought_qty)
    sold_qty = round_down(_safe_float(sell.get("executedQty")), step_size)
    sell_quote = _safe_float(sell.get("cummulativeQuoteQty"))
    if sell_quote <= 0:
        sell_quote = sold_qty * bid_price
    return {
        "ok": True,
        "buy_qty": bought_qty,
        "sell_qty": sold_qty,
        "buy_quote": round(buy_quote, 12),
        "sell_quote": round(sell_quote, 12),
        "gross_notional": round(buy_quote + sell_quote, 12),
        "price_wear": round(max(buy_quote - sell_quote, 0.0), 12),
        "residual_base_qty": round(bought_qty - sold_qty, 12),
        "buy_order_id": buy.get("orderId"),
        "sell_order_id": sell.get("orderId"),
    }


def _book(symbol: str) -> dict[str, float]:
    rows = fetch_spot_book_tickers(symbol=symbol)
    if not rows:
        raise RuntimeError(f"empty spot book for {symbol}")
    row = rows[0]
    bid = _safe_float(row.get("bid_price") or row.get("bidPrice"))
    ask = _safe_float(row.get("ask_price") or row.get("askPrice"))
    if bid <= 0 or ask <= 0 or ask < bid:
        raise RuntimeError(f"invalid spot book for {symbol}: bid={bid} ask={ask}")
    mid = (bid + ask) / 2.0
    return {"bid": bid, "ask": ask, "mid": mid, "spread_bps": (ask - bid) / mid * 10_000.0}


def _free_balance(account: dict[str, Any], asset: str) -> float:
    for item in account.get("balances", []):
        if isinstance(item, dict) and str(item.get("asset", "")).upper() == asset.upper():
            return _safe_float(item.get("free"))
    return 0.0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Controlled Binance spot MARKET buy/sell volume roundtrips.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--target-volume", required=True, type=float)
    parser.add_argument("--per-leg-notional", type=float, default=100.0)
    parser.add_argument("--sleep-seconds", type=float, default=5.0)
    parser.add_argument("--max-spread-bps", type=float, default=10.0)
    parser.add_argument("--max-residual-notional", type=float, default=5.0)
    parser.add_argument("--max-total-price-wear", type=float, default=50.0)
    parser.add_argument("--max-roundtrips", type=int, default=500)
    parser.add_argument("--status-json", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    symbol = str(args.symbol).upper().strip()
    target_volume = max(float(args.target_volume), 0.0)
    if not symbol or target_volume <= 0:
        raise SystemExit("symbol and positive target-volume are required")
    config = fetch_spot_symbol_config(symbol)
    step_size = max(_safe_float(config.get("step_size")), 0.0)
    min_qty = max(_safe_float(config.get("min_qty")), 0.0)
    min_notional = max(_safe_float(config.get("min_notional")), 0.0)
    base_asset = str(config.get("base_asset") or symbol.removesuffix("USDT")).upper()
    quote_asset = str(config.get("quote_asset") or "USDT").upper()
    creds = load_binance_api_credentials()
    if args.apply and not creds:
        raise SystemExit("Binance API credentials required for --apply")
    api_key, api_secret = creds or ("", "")
    status_path = Path(args.status_json) if str(args.status_json).strip() else Path("output") / f"{symbol.lower()}_take_roundtrip_status.json"

    done_volume = 0.0
    price_wear = 0.0
    residual_base = 0.0
    roundtrips = 0
    started_at = datetime.now(timezone.utc).isoformat()

    def emit(reason: str, **extra: Any) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at,
            "symbol": symbol,
            "apply": bool(args.apply),
            "reason": reason,
            "done_volume": round(done_volume, 8),
            "target_volume": target_volume,
            "remaining_volume": round(max(target_volume - done_volume, 0.0), 8),
            "price_wear": round(price_wear, 8),
            "price_wear_per_10k": round(price_wear / done_volume * 10_000.0, 8) if done_volume > 0 else 0.0,
            "residual_base_qty": round(residual_base, 12),
            "roundtrips": roundtrips,
            **extra,
        }
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(json.dumps(record, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(record, ensure_ascii=True, sort_keys=True), flush=True)

    if args.apply:
        account = fetch_spot_account_info(api_key, api_secret)
        emit(
            "preflight",
            base_asset=base_asset,
            quote_asset=quote_asset,
            base_free=_free_balance(account, base_asset),
            quote_free=_free_balance(account, quote_asset),
        )

    while done_volume + 1e-9 < target_volume:
        if roundtrips >= max(int(args.max_roundtrips), 1):
            emit("max_roundtrips_kill")
            return 2
        book = _book(symbol)
        if args.max_spread_bps > 0 and book["spread_bps"] > args.max_spread_bps:
            emit("spread_wide_wait", spread_bps=book["spread_bps"])
            time.sleep(max(float(args.sleep_seconds), 0.5))
            continue
        leg_notional = next_leg_notional(
            remaining_volume=target_volume - done_volume,
            per_leg_notional=float(args.per_leg_notional),
            min_notional=minimum_executable_leg_notional(
                min_notional=min_notional,
                ask_price=book["ask"],
                step_size=step_size,
            ),
        )

        sequence = roundtrips + 1

        def submit(side: str, qty: float) -> dict[str, Any]:
            if not args.apply:
                price = book["ask"] if side == "BUY" else book["bid"]
                return {"executedQty": qty, "cummulativeQuoteQty": qty * price, "orderId": f"dry-{side}-{sequence}"}
            client_id = f"tk-{symbol.lower()[:8]}-{side[0].lower()}-{int(time.time() * 1000) % 100000000:08d}"[:36]
            return post_spot_order(
                symbol=symbol,
                side=side,
                quantity=qty,
                price=0.0,
                api_key=api_key,
                api_secret=api_secret,
                order_type="MARKET",
                new_client_order_id=client_id,
            )

        try:
            result = execute_take_roundtrip(
                leg_notional=leg_notional,
                bid_price=book["bid"],
                ask_price=book["ask"],
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
                submit_market=submit,
            )
        except Exception as exc:
            emit("roundtrip_submit_unknown_kill", error=f"{type(exc).__name__}: {exc}")
            return 2
        if not result.get("ok"):
            emit("roundtrip_not_filled_kill", result=result)
            return 2
        done_volume += _safe_float(result.get("gross_notional"))
        price_wear += _safe_float(result.get("price_wear"))
        residual_base += _safe_float(result.get("residual_base_qty"))
        roundtrips += 1
        emit("roundtrip", spread_bps=book["spread_bps"], last=result)
        if price_wear > max(float(args.max_total_price_wear), 0.0):
            emit("max_total_price_wear_kill")
            return 2
        if abs(residual_base) * book["mid"] > max(float(args.max_residual_notional), 0.0):
            emit("max_residual_notional_kill")
            return 2
        if done_volume + 1e-9 < target_volume:
            time.sleep(max(float(args.sleep_seconds), 0.0))

    emit("target_reached")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
