"""Exact, half-open Binance futures trade-window pagination.

All lifecycle consumers use this one paging primitive so a target gate,
runner guard, and terminal receipt cannot disagree about which exchange fills
exist.  Callers remain responsible for validating business fields such as
notional or realized PnL.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


TradePageFetcher = Callable[[int, int, int], list[dict[str, Any]]]


def fetch_exact_futures_trade_window(
    *,
    fetch_page: TradePageFetcher,
    start_time_ms: int,
    end_time_exclusive_ms: int,
    page_limit: int = 1000,
    max_requests: int = 80,
) -> list[dict[str, Any]]:
    """Return complete trades in ``[start_time_ms, end_time_exclusive_ms)``.

    Binance exposes an inclusive ``endTime`` and a finite page size.  A full
    page is therefore recursively split into disjoint time ranges.  A full
    one-millisecond page cannot be proven complete and fails closed.
    """

    start_ms = int(start_time_ms)
    end_exclusive_ms = int(end_time_exclusive_ms)
    limit = int(page_limit)
    request_limit = int(max_requests)
    if end_exclusive_ms <= start_ms:
        return []
    if start_ms < 0 or limit <= 0 or request_limit <= 0:
        raise ValueError("futures trade window pagination is invalid")

    pending_ranges: list[tuple[int, int]] = [(start_ms, end_exclusive_ms)]
    request_count = 0
    rows: list[dict[str, Any]] = []
    seen_trade_ids: set[str] = set()
    while pending_ranges:
        if request_count >= request_limit:
            raise ValueError("futures trade window request limit exhausted")
        range_start_ms, range_end_exclusive_ms = pending_ranges.pop()
        request_count += 1
        batch = fetch_page(
            range_start_ms,
            range_end_exclusive_ms - 1,
            limit,
        )
        if not isinstance(batch, list):
            raise ValueError("futures trade window page must be a list")
        if len(batch) > limit:
            raise ValueError("futures trade window page exceeds requested limit")
        if len(batch) == limit:
            if range_end_exclusive_ms - range_start_ms <= 1:
                raise ValueError(
                    "futures trade window density exceeds timestamp resolution"
                )
            midpoint_ms = range_start_ms + (
                range_end_exclusive_ms - range_start_ms
            ) // 2
            pending_ranges.append((midpoint_ms, range_end_exclusive_ms))
            pending_ranges.append((range_start_ms, midpoint_ms))
            continue

        for raw_row in batch:
            if not isinstance(raw_row, dict):
                raise ValueError("futures trade window row must be an object")
            trade_id = raw_row.get("id")
            if trade_id is None or not str(trade_id).strip():
                raise ValueError("futures trade id is required")
            try:
                trade_time_ms = int(raw_row.get("time"))
            except (TypeError, ValueError) as exc:
                raise ValueError("futures trade window time is invalid") from exc
            if not (
                range_start_ms
                <= trade_time_ms
                < range_end_exclusive_ms
            ):
                raise ValueError(
                    "futures trade window row fell outside requested page"
                )
            normalized_id = str(trade_id)
            if normalized_id in seen_trade_ids:
                continue
            seen_trade_ids.add(normalized_id)
            rows.append(dict(raw_row))

    rows.sort(key=lambda row: (int(row["time"]), str(row["id"])))
    return rows
