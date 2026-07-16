from datetime import datetime, timedelta, timezone

import pytest

import grid_optimizer.competition_target_gate as target_gate


START = datetime(2026, 7, 16, 1, 0, tzinfo=timezone.utc)
END = START + timedelta(hours=1)


def test_target_gate_counts_only_half_open_run_window(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    def fetch(**kwargs):
        captured.append(kwargs)
        return [
            {"id": 2, "time": int(START.timestamp() * 1000), "quoteQty": "1000", "realizedPnl": "-1"},
        ]

    monkeypatch.setattr(target_gate, "fetch_futures_user_trades", fetch)

    stats = target_gate.daily_vol_wear(
        "BCHUSDT",
        "key",
        "secret",
        window_start=START,
        window_end=END,
        now=END + timedelta(minutes=5),
    )

    assert stats["gross_notional"] == pytest.approx(1000.0)
    assert stats["realized_pnl"] == pytest.approx(-1.0)
    assert stats["wear_per_10k"] == pytest.approx(10.0)
    assert stats["trade_count"] == 1
    assert captured[0]["start_time_ms"] == int(START.timestamp() * 1000)
    assert captured[0]["end_time_ms"] == int(END.timestamp() * 1000) - 1
    assert captured[0]["use_cache"] is False


def test_target_gate_bisects_full_pages_without_timestamp_cursor_loss(monkeypatch) -> None:
    start_ms = int(START.timestamp() * 1000)
    end_ms = int(END.timestamp() * 1000)
    midpoint_ms = start_ms + (end_ms - start_ms) // 2
    captured: list[tuple[int, int]] = []

    def fetch(**kwargs):
        page_start = int(kwargs["start_time_ms"])
        page_end = int(kwargs["end_time_ms"])
        captured.append((page_start, page_end))
        if page_start == start_ms and page_end == end_ms - 1:
            return [
                {
                    "id": index,
                    "time": start_ms + index,
                    "quoteQty": "1",
                    "realizedPnl": "0",
                }
                for index in range(1000)
            ]
        trade_time = page_start
        return [
            {
                "id": f"{page_start}",
                "time": trade_time,
                "quoteQty": "10",
                "realizedPnl": "0",
            }
        ]

    monkeypatch.setattr(target_gate, "fetch_futures_user_trades", fetch)

    stats = target_gate.daily_vol_wear(
        "BCHUSDT",
        "key",
        "secret",
        window_start=START,
        window_end=END,
        now=END,
    )

    assert stats["gross_notional"] == pytest.approx(20.0)
    assert captured == [
        (start_ms, end_ms - 1),
        (start_ms, midpoint_ms - 1),
        (midpoint_ms, end_ms - 1),
    ]


def test_target_gate_fails_closed_when_one_millisecond_page_is_full(monkeypatch) -> None:
    one_millisecond_end = START + timedelta(milliseconds=1)
    trade_time = int(START.timestamp() * 1000)
    monkeypatch.setattr(
        target_gate,
        "fetch_futures_user_trades",
        lambda **kwargs: [
            {
                "id": index,
                "time": trade_time,
                "quoteQty": "1",
                "realizedPnl": "0",
            }
            for index in range(1000)
        ],
    )

    with pytest.raises(ValueError, match="timestamp resolution"):
        target_gate.daily_vol_wear(
            "BCHUSDT",
            "key",
            "secret",
            window_start=START,
            window_end=one_millisecond_end,
            now=one_millisecond_end,
        )


def test_target_gate_rejects_exchange_rows_outside_requested_slice(monkeypatch) -> None:
    monkeypatch.setattr(
        target_gate,
        "fetch_futures_user_trades",
        lambda **kwargs: [
            {
                "id": 1,
                "time": int(END.timestamp() * 1000),
                "quoteQty": "1",
                "realizedPnl": "0",
            }
        ],
    )

    with pytest.raises(ValueError, match="outside requested page"):
        target_gate.daily_vol_wear(
            "BCHUSDT",
            "key",
            "secret",
            window_start=START,
            window_end=END,
            now=END,
        )


def test_target_gate_rejects_trade_without_exchange_id(monkeypatch) -> None:
    monkeypatch.setattr(
        target_gate,
        "fetch_futures_user_trades",
        lambda **kwargs: [
            {
                "time": int(START.timestamp() * 1000),
                "quoteQty": "1000",
                "realizedPnl": "0",
            }
        ],
    )

    with pytest.raises(ValueError, match="trade id"):
        target_gate.daily_vol_wear(
            "BCHUSDT",
            "key",
            "secret",
            window_start=START,
            window_end=END,
            now=END,
        )


def test_target_gate_rejects_trade_without_realized_pnl(monkeypatch) -> None:
    monkeypatch.setattr(
        target_gate,
        "fetch_futures_user_trades",
        lambda **kwargs: [
            {
                "id": 1,
                "time": int(START.timestamp() * 1000),
                "quoteQty": "1000",
            }
        ],
    )

    with pytest.raises(ValueError, match="realizedPnl is required"):
        target_gate.daily_vol_wear(
            "BCHUSDT",
            "key",
            "secret",
            window_start=START,
            window_end=END,
            now=END,
        )


def test_target_gate_rejects_trade_without_provable_notional(monkeypatch) -> None:
    monkeypatch.setattr(
        target_gate,
        "fetch_futures_user_trades",
        lambda **kwargs: [
            {
                "id": 1,
                "time": int(START.timestamp() * 1000),
                "realizedPnl": "0",
            }
        ],
    )

    with pytest.raises(ValueError, match="notional is missing"):
        target_gate.daily_vol_wear(
            "BCHUSDT",
            "key",
            "secret",
            window_start=START,
            window_end=END,
            now=END,
        )


def test_terminal_intent_id_is_scoped_to_run_contract() -> None:
    first = target_gate._intent_id(
        symbol="BCHUSDT",
        trigger_reason="target_reached",
        run_contract_id="contract-one",
    )
    second = target_gate._intent_id(
        symbol="BCHUSDT",
        trigger_reason="target_reached",
        run_contract_id="contract-two",
    )

    assert first != second
