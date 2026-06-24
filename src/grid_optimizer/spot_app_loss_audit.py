from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from typing import Any

from .data import (
    fetch_spot_book_tickers,
    fetch_spot_symbol_config,
    fetch_spot_user_trades,
    load_binance_api_credentials,
)

EPSILON = 1e-12


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _parse_time_ms(value: str | None) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        return int(raw)
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


def _ceil_to_tick(value: float, tick_size: float | None) -> float:
    tick = max(_safe_float(tick_size), 0.0)
    if value <= EPSILON or tick <= EPSILON:
        return max(value, 0.0)
    return math.ceil((value - EPSILON) / tick) * tick


def compute_spot_app_loss_audit(
    *,
    trades: list[dict[str, Any]],
    bid_price: float,
    ask_price: float,
    tick_size: float | None = None,
) -> dict[str, Any]:
    buy_notional = 0.0
    sell_notional = 0.0
    buy_qty = 0.0
    sell_qty = 0.0
    maker_count = 0
    first_trade_time_ms = 0
    last_trade_time_ms = 0
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        qty = max(_safe_float(trade.get("qty")), 0.0)
        price = max(_safe_float(trade.get("price")), 0.0)
        notional = max(_safe_float(trade.get("quoteQty")), 0.0)
        if notional <= EPSILON:
            notional = qty * price
        if qty <= EPSILON or notional <= EPSILON:
            continue
        if _truthy(trade.get("isBuyer")):
            buy_qty += qty
            buy_notional += notional
        else:
            sell_qty += qty
            sell_notional += notional
        if _truthy(trade.get("isMaker")):
            maker_count += 1
        trade_time_ms = _safe_int(trade.get("time"))
        if trade_time_ms > 0:
            first_trade_time_ms = trade_time_ms if first_trade_time_ms <= 0 else min(first_trade_time_ms, trade_time_ms)
            last_trade_time_ms = max(last_trade_time_ms, trade_time_ms)

    gross_notional = buy_notional + sell_notional
    net_qty = buy_qty - sell_qty
    safe_bid = max(_safe_float(bid_price), 0.0)
    safe_ask = max(_safe_float(ask_price), 0.0)
    mid_price = (safe_bid + safe_ask) / 2.0 if safe_bid > EPSILON and safe_ask > EPSILON else max(safe_bid, safe_ask)
    mark_price = safe_bid if net_qty > EPSILON and safe_bid > EPSILON else mid_price
    holding_qty = max(net_qty, 0.0)
    position_value = holding_qty * mark_price
    raw_app_loss = buy_notional - sell_notional - position_value
    app_loss = max(raw_app_loss, 0.0)
    app_loss_per_10k = app_loss / gross_notional * 10000.0 if gross_notional > EPSILON else 0.0
    break_even_price = (buy_notional - sell_notional) / net_qty if net_qty > EPSILON else 0.0
    safe_maker_sell_price = _ceil_to_tick(max(break_even_price, safe_ask), tick_size) if net_qty > EPSILON else 0.0
    tick = max(_safe_float(tick_size), 0.0)
    sell_gap_ticks = ((safe_maker_sell_price - safe_ask) / tick) if tick > EPSILON and safe_maker_sell_price > EPSILON else 0.0
    return {
        "trade_count": len([item for item in trades if isinstance(item, dict)]),
        "maker_count": maker_count,
        "buy_notional": buy_notional,
        "sell_notional": sell_notional,
        "gross_notional": gross_notional,
        "buy_qty": buy_qty,
        "sell_qty": sell_qty,
        "net_qty": net_qty,
        "holding_qty": holding_qty,
        "bid_price": safe_bid,
        "ask_price": safe_ask,
        "mark_price": mark_price,
        "mark_source": "bid" if net_qty > EPSILON and safe_bid > EPSILON else "mid",
        "position_value": position_value,
        "raw_app_loss": raw_app_loss,
        "app_loss": app_loss,
        "app_loss_per_10k": app_loss_per_10k,
        "break_even_price": break_even_price,
        "safe_maker_sell_price": safe_maker_sell_price,
        "safe_maker_sell_gap_ticks": sell_gap_ticks,
        "first_trade_time_ms": first_trade_time_ms,
        "last_trade_time_ms": last_trade_time_ms,
    }


def evaluate_spot_app_loss_recovery_gate(
    audit: dict[str, Any],
    *,
    max_app_loss_per_10k: float = 10.0,
    max_safe_maker_sell_gap_ticks: float = 2.0,
    min_maker_ratio: float = 0.99,
    min_gross_notional: float = 0.0,
) -> dict[str, Any]:
    reasons: list[str] = []
    trade_count = _safe_int(audit.get("trade_count"))
    maker_count = _safe_int(audit.get("maker_count"))
    maker_ratio = (maker_count / trade_count) if trade_count > 0 else 0.0
    gross_notional = _safe_float(audit.get("gross_notional"))
    app_loss_per_10k = _safe_float(audit.get("app_loss_per_10k"))
    net_qty = _safe_float(audit.get("net_qty"))
    safe_sell_gap_ticks = max(_safe_float(audit.get("safe_maker_sell_gap_ticks")), 0.0)
    if trade_count <= 0:
        reasons.append("no_trades")
    if bool(audit.get("truncated")):
        reasons.append("trade_window_truncated")
    if gross_notional + EPSILON < max(_safe_float(min_gross_notional), 0.0):
        reasons.append("gross_notional_below_min")
    if app_loss_per_10k > max(_safe_float(max_app_loss_per_10k), 0.0) + EPSILON:
        reasons.append("app_loss_per_10k_above_limit")
    if trade_count > 0 and maker_ratio + EPSILON < max(_safe_float(min_maker_ratio), 0.0):
        reasons.append("maker_ratio_below_min")
    if net_qty > EPSILON and safe_sell_gap_ticks > max(_safe_float(max_safe_maker_sell_gap_ticks), 0.0) + EPSILON:
        reasons.append("safe_maker_sell_too_far")
    return {
        "allowed": not reasons,
        "reasons": reasons,
        "max_app_loss_per_10k": max(_safe_float(max_app_loss_per_10k), 0.0),
        "max_safe_maker_sell_gap_ticks": max(_safe_float(max_safe_maker_sell_gap_ticks), 0.0),
        "min_maker_ratio": max(_safe_float(min_maker_ratio), 0.0),
        "min_gross_notional": max(_safe_float(min_gross_notional), 0.0),
        "maker_ratio": maker_ratio,
        "app_loss_per_10k": app_loss_per_10k,
        "safe_maker_sell_gap_ticks": safe_sell_gap_ticks,
        "gross_notional": gross_notional,
        "net_qty": net_qty,
    }


def build_live_spot_app_loss_audit(
    *,
    symbol: str,
    start_time_ms: int | None,
    end_time_ms: int | None,
    limit: int = 1000,
) -> dict[str, Any]:
    creds = load_binance_api_credentials()
    if not creds:
        raise RuntimeError("missing Binance API credentials")
    trades = fetch_spot_user_trades(
        symbol=symbol,
        api_key=creds[0],
        api_secret=creds[1],
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=limit,
    )
    book_rows = fetch_spot_book_tickers(symbol=symbol)
    if not book_rows:
        raise RuntimeError(f"spot bookTicker returned empty for {symbol}")
    symbol_config = fetch_spot_symbol_config(symbol)
    book = book_rows[0]
    audit = compute_spot_app_loss_audit(
        trades=trades,
        bid_price=_safe_float(book.get("bid_price")),
        ask_price=_safe_float(book.get("ask_price")),
        tick_size=symbol_config.get("tick_size"),
    )
    audit.update(
        {
            "symbol": symbol.upper(),
            "start_time_ms": start_time_ms,
            "end_time_ms": end_time_ms,
            "limit": int(limit),
            "truncated": len(trades) >= int(limit),
            "tick_size": symbol_config.get("tick_size"),
        }
    )
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit Binance spot APP loss formula from myTrades.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-time", default="", help="ISO datetime or millisecond timestamp.")
    parser.add_argument("--end-time", default="", help="ISO datetime or millisecond timestamp.")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--max-app-loss-per-10k", type=float, default=10.0)
    parser.add_argument("--max-safe-maker-sell-gap-ticks", type=float, default=2.0)
    parser.add_argument("--min-maker-ratio", type=float, default=0.99)
    parser.add_argument("--min-gross-notional", type=float, default=0.0)
    args = parser.parse_args()
    audit = build_live_spot_app_loss_audit(
        symbol=str(args.symbol).upper().strip(),
        start_time_ms=_parse_time_ms(args.start_time),
        end_time_ms=_parse_time_ms(args.end_time),
        limit=max(min(int(args.limit), 1000), 1),
    )
    audit["recovery_gate"] = evaluate_spot_app_loss_recovery_gate(
        audit,
        max_app_loss_per_10k=args.max_app_loss_per_10k,
        max_safe_maker_sell_gap_ticks=args.max_safe_maker_sell_gap_ticks,
        min_maker_ratio=args.min_maker_ratio,
        min_gross_notional=args.min_gross_notional,
    )
    print(json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
