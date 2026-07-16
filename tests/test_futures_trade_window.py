from __future__ import annotations

import pytest

from grid_optimizer.futures_trade_window import (
    fetch_exact_futures_trade_window,
)


def test_trade_window_keeps_partial_fills_and_deduplicates_trade_id() -> None:
    rows = [
        {"id": 1, "orderId": 77, "time": 1000},
        {"id": 2, "orderId": 77, "time": 1001},
        {"id": 2, "orderId": 77, "time": 1001},
    ]

    observed = fetch_exact_futures_trade_window(
        fetch_page=lambda start, end, limit: [
            row for row in rows if start <= row["time"] <= end
        ],
        start_time_ms=1000,
        end_time_exclusive_ms=1002,
    )

    assert [row["id"] for row in observed] == [1, 2]
    assert {row["orderId"] for row in observed} == {77}


def test_trade_window_excludes_exact_end_boundary() -> None:
    rows = [
        {"id": 1, "time": 1000},
        {"id": 2, "time": 1999},
        {"id": 3, "time": 2000},
    ]

    observed = fetch_exact_futures_trade_window(
        fetch_page=lambda start, end, limit: [
            row for row in rows if start <= row["time"] <= end
        ],
        start_time_ms=1000,
        end_time_exclusive_ms=2000,
    )

    assert [row["id"] for row in observed] == [1, 2]


def test_trade_window_bisects_full_pages_without_overlap() -> None:
    rows = [
        {"id": 1, "time": 1000},
        {"id": 2, "time": 1001},
        {"id": 3, "time": 1002},
        {"id": 4, "time": 1003},
    ]

    observed = fetch_exact_futures_trade_window(
        fetch_page=lambda start, end, limit: [
            row for row in rows if start <= row["time"] <= end
        ][:limit],
        start_time_ms=1000,
        end_time_exclusive_ms=1004,
        page_limit=2,
    )

    assert [row["id"] for row in observed] == [1, 2, 3, 4]


def test_trade_window_full_single_millisecond_fails_closed() -> None:
    with pytest.raises(ValueError, match="density exceeds"):
        fetch_exact_futures_trade_window(
            fetch_page=lambda start, end, limit: [
                {"id": index, "time": start} for index in range(limit)
            ],
            start_time_ms=1000,
            end_time_exclusive_ms=1001,
            page_limit=2,
        )
