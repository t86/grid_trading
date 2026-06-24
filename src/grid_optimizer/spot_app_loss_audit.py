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
MAX_SPOT_MY_TRADES_WINDOW_MS = 24 * 60 * 60 * 1000


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


def _trade_identity(trade: dict[str, Any]) -> tuple[Any, ...]:
    trade_id = trade.get("id")
    if trade_id is not None:
        return ("id", str(trade_id))
    return (
        "fallback",
        _safe_int(trade.get("time")),
        str(trade.get("isBuyer")),
        str(trade.get("price")),
        str(trade.get("qty")),
        str(trade.get("quoteQty")),
    )


def _sort_trade_key(trade: dict[str, Any]) -> tuple[int, int]:
    return (_safe_int(trade.get("time")), _safe_int(trade.get("id")))


def _fetch_spot_user_trades_interval(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
) -> tuple[list[dict[str, Any]], bool, int]:
    intervals: list[tuple[int, int]] = [(start_time_ms, end_time_ms)]
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    truncated = False
    request_count = 0
    while intervals:
        interval_start, interval_end = intervals.pop(0)
        if interval_end < interval_start:
            continue
        trades = fetch_spot_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=interval_start,
            end_time_ms=interval_end,
            limit=limit,
        )
        request_count += 1
        valid_trades = [trade for trade in trades if isinstance(trade, dict)]
        for trade in valid_trades:
            seen[_trade_identity(trade)] = trade
        if len(valid_trades) < limit:
            continue
        trade_times = [_safe_int(trade.get("time")) for trade in valid_trades if _safe_int(trade.get("time")) > 0]
        if not trade_times:
            truncated = True
            continue
        first_time = min(trade_times)
        last_time = max(trade_times)
        split = False
        if first_time > interval_start:
            intervals.append((interval_start, first_time - 1))
            split = True
        if last_time < interval_end:
            intervals.append((last_time + 1, interval_end))
            split = True
        if not split:
            truncated = True
    return sorted(seen.values(), key=_sort_trade_key), truncated, request_count


def _fetch_spot_user_trades_paginated(
    *,
    symbol: str,
    api_key: str,
    api_secret: str,
    start_time_ms: int | None,
    end_time_ms: int | None,
    limit: int,
) -> tuple[list[dict[str, Any]], bool, int]:
    safe_limit = max(min(int(limit), 1000), 1)
    if start_time_ms is None or end_time_ms is None:
        trades = fetch_spot_user_trades(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=safe_limit,
        )
        return trades, len(trades) >= safe_limit, 1
    if end_time_ms < start_time_ms:
        return [], False, 0

    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    truncated = False
    request_count = 0
    cursor = start_time_ms
    while cursor <= end_time_ms:
        window_end = min(cursor + MAX_SPOT_MY_TRADES_WINDOW_MS - 1, end_time_ms)
        trades, window_truncated, window_requests = _fetch_spot_user_trades_interval(
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            start_time_ms=cursor,
            end_time_ms=window_end,
            limit=safe_limit,
        )
        request_count += window_requests
        truncated = truncated or window_truncated
        for trade in trades:
            seen[_trade_identity(trade)] = trade
        cursor = window_end + 1
    return sorted(seen.values(), key=_sort_trade_key), truncated, request_count


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
    bid_break_even_buffer_ticks = (
        (safe_bid - break_even_price) / tick
        if net_qty > EPSILON and tick > EPSILON and break_even_price > EPSILON and safe_bid > EPSILON
        else 0.0
    )
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
        "bid_break_even_buffer_ticks": bid_break_even_buffer_ticks,
        "first_trade_time_ms": first_trade_time_ms,
        "last_trade_time_ms": last_trade_time_ms,
    }


def evaluate_spot_app_loss_recovery_gate(
    audit: dict[str, Any],
    *,
    max_app_loss_per_10k: float = 10.0,
    max_safe_maker_sell_gap_ticks: float = 2.0,
    min_bid_break_even_buffer_ticks: float = 0.0,
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
    bid_buffer_ticks = _safe_float(audit.get("bid_break_even_buffer_ticks"))
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
    min_bid_buffer = max(_safe_float(min_bid_break_even_buffer_ticks), 0.0)
    if net_qty > EPSILON and min_bid_buffer > EPSILON and bid_buffer_ticks + EPSILON < min_bid_buffer:
        reasons.append("bid_break_even_buffer_below_min")
    return {
        "allowed": not reasons,
        "reasons": reasons,
        "max_app_loss_per_10k": max(_safe_float(max_app_loss_per_10k), 0.0),
        "max_safe_maker_sell_gap_ticks": max(_safe_float(max_safe_maker_sell_gap_ticks), 0.0),
        "min_bid_break_even_buffer_ticks": min_bid_buffer,
        "min_maker_ratio": max(_safe_float(min_maker_ratio), 0.0),
        "min_gross_notional": max(_safe_float(min_gross_notional), 0.0),
        "maker_ratio": maker_ratio,
        "app_loss_per_10k": app_loss_per_10k,
        "safe_maker_sell_gap_ticks": safe_sell_gap_ticks,
        "bid_break_even_buffer_ticks": bid_buffer_ticks,
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
    if start_time_ms is not None and end_time_ms is None:
        end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    trades, truncated, request_count = _fetch_spot_user_trades_paginated(
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
            "truncated": truncated,
            "request_count": request_count,
            "tick_size": symbol_config.get("tick_size"),
        }
    )
    return audit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit Binance spot APP loss formula from myTrades.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-time", default="", help="ISO datetime or millisecond timestamp.")
    parser.add_argument("--end-time", default="", help="ISO datetime or millisecond timestamp.")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--max-app-loss-per-10k", type=float, default=10.0)
    parser.add_argument("--max-safe-maker-sell-gap-ticks", type=float, default=2.0)
    parser.add_argument("--min-bid-break-even-buffer-ticks", type=float, default=0.0)
    parser.add_argument("--min-maker-ratio", type=float, default=0.99)
    parser.add_argument("--min-gross-notional", type=float, default=0.0)
    parser.add_argument("--require-gate", action="store_true", help="Exit non-zero when recovery_gate.allowed is false.")
    args = parser.parse_args(argv)
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
        min_bid_break_even_buffer_ticks=args.min_bid_break_even_buffer_ticks,
        min_maker_ratio=args.min_maker_ratio,
        min_gross_notional=args.min_gross_notional,
    )
    print(json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True))
    if bool(args.require_gate) and not bool(audit["recovery_gate"].get("allowed")):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
