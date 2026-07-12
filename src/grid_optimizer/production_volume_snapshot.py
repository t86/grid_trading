from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from .bq_volume_recovery_guard import (
    fetch_recent_user_trades,
    summarize_recent_realized_pnl,
    summarize_recent_volume,
)
from .data import fetch_futures_user_trades


WINDOW_MINUTES = (5, 10, 15, 30, 60)


def _dedupe_canonical_trades(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        trade_id = row.get("id")
        if trade_id is None or not str(trade_id).strip():
            raise ValueError("exchange userTrades row is missing canonical trade id")
        unique.setdefault(str(trade_id).strip(), row)
    return list(unique.values())


def build_exchange_volume_snapshot(
    *,
    symbol: str,
    rows: list[dict[str, Any]],
    now: datetime,
    target_notional: float | None = None,
    completion_buffer_seconds: float = 10_800.0,
) -> dict[str, Any]:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now_utc = now.astimezone(timezone.utc)
    canonical_rows = _dedupe_canonical_trades(rows)
    day_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_seconds = max((now_utc - day_start).total_seconds(), 1.0)
    day_volume = summarize_recent_volume(rows=canonical_rows, now=now_utc, window_seconds=elapsed_seconds)
    day_realized = summarize_recent_realized_pnl(
        rows=canonical_rows,
        now=now_utc,
        window_seconds=elapsed_seconds,
    )

    windows: dict[str, dict[str, Any]] = {}
    for minutes in WINDOW_MINUTES:
        seconds = float(minutes * 60)
        volume = summarize_recent_volume(rows=canonical_rows, now=now_utc, window_seconds=seconds)
        windows[f"{minutes}m"] = {
            "gross_quote_qty": volume["gross_notional"],
            "hourly_rate": volume["gross_notional"] * 3600.0 / seconds,
            "trade_count": volume["trade_count"],
            "realized_pnl": summarize_recent_realized_pnl(
                rows=canonical_rows,
                now=now_utc,
                window_seconds=seconds,
            ),
            "latest_trade_at": volume["latest_trade_at"],
        }

    target = max(float(target_notional or 0.0), 0.0)
    deadline = day_start + timedelta(days=1, seconds=-max(float(completion_buffer_seconds), 0.0))
    remaining_seconds = max((deadline - now_utc).total_seconds(), 0.0)
    day_gross = float(day_volume["gross_notional"])
    remaining_target = max(target - day_gross, 0.0)
    required_hourly_rate = remaining_target * 3600.0 / remaining_seconds if remaining_seconds > 0 else None
    return {
        "canonical": True,
        "source": "binance_futures_userTrades",
        "dedupe_key": "exchange_trade_id",
        "symbol": symbol.upper().strip(),
        "generated_at": now_utc.isoformat(),
        "day_start": day_start.isoformat(),
        "target_deadline": deadline.isoformat(),
        "day": {
            "gross_quote_qty": day_gross,
            "trade_count": day_volume["trade_count"],
            "realized_pnl": day_realized,
            "target_notional": target,
            "remaining_target": remaining_target,
            "required_hourly_rate": required_hourly_rate,
        },
        "windows": windows,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Canonical production volume snapshot from Binance Futures userTrades only."
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--target", type=float, default=0.0)
    parser.add_argument("--completion-buffer-seconds", type=float, default=10_800.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        api_key = os.environ["BINANCE_API_KEY"]
        api_secret = os.environ["BINANCE_API_SECRET"]
        rows = fetch_recent_user_trades(
            fetch_page=lambda **kwargs: fetch_futures_user_trades(
                symbol=args.symbol,
                api_key=api_key,
                api_secret=api_secret,
                **kwargs,
            ),
            now=now,
            window_seconds=max((now - day_start).total_seconds(), 1.0),
        )
        snapshot = build_exchange_volume_snapshot(
            symbol=args.symbol,
            rows=rows,
            now=now,
            target_notional=args.target,
            completion_buffer_seconds=args.completion_buffer_seconds,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "canonical": False,
                    "source": "binance_futures_userTrades",
                    "error": f"{type(exc).__name__}: {exc}",
                },
                ensure_ascii=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(snapshot, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
