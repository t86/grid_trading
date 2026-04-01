#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from grid_optimizer.short_volume_candidates import build_short_volume_candidate_report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search short-only volume-oriented grid candidates and backtest them on recent Binance futures data."
    )
    parser.add_argument("--symbol", type=str, default="XAUTUSDT")
    parser.add_argument("--days", type=float, default=2.0, help="Rolling backtest window in days")
    parser.add_argument("--interval", type=str, default="1m")
    parser.add_argument("--budget", type=float, default=10_000.0, help="Reference capital notional for backtest")
    parser.add_argument("--contract-type", type=str, default="usdm")
    parser.add_argument("--cache-dir", type=str, default="data")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--output-json",
        type=str,
        default="",
        help="Optional JSON report path",
    )
    return parser


def _print_report(report: dict[str, Any]) -> None:
    symbol = str(report.get("symbol", ""))
    context = report.get("context", {})
    funding_snapshot = report.get("current_funding_snapshot")
    candidates = report.get("candidates", [])
    print(f"Symbol: {symbol}")
    print(f"Window (UTC): {context['start_time_utc']} -> {context['end_time_utc']}")
    print(f"Interval: {context['interval']} | Candles: {context['candle_count']}")
    print(
        f"Reference close: {context['reference_price']:.4f} | "
        f"Window low/high: {context['window_low']:.4f} / {context['window_high']:.4f}"
    )
    print(
        f"Window return: {context['window_return'] * 100:.2f}% | "
        f"Funding events: {context['funding_events']} | "
        f"Funding rate sum: {context['funding_sum'] * 100:.4f}%"
    )
    print(f"Budget baseline: {context['budget']:.2f} USDT")
    if funding_snapshot is not None:
        print(
            "Current funding snapshot: "
            f"{(funding_snapshot.get('funding_rate') or 0.0) * 100:.4f}% "
            f"(mark {funding_snapshot.get('mark_price')}, "
            f"next {funding_snapshot.get('next_funding_time')})"
        )
    print("")
    print("=== Short Candidate Report ===")
    for idx, candidate in enumerate(candidates, start=1):
        params = candidate.get("params", {})
        metrics = candidate.get("metrics", {})
        print(f"{idx}. {candidate.get('title', candidate.get('label', '-'))}")
        print(f"   {candidate.get('description', '-')}")
        print(
            "   "
            f"range={params.get('min_price', 0.0):.4f}-{params.get('max_price', 0.0):.4f} "
            f"({params.get('min_offset_pct', 0.0):.2f}% / +{params.get('max_offset_pct', 0.0):.2f}%) "
            f"n={params.get('n')} mode={params.get('grid_level_mode')}/{params.get('allocation_mode')}"
        )
        print(
            "   "
            f"turnover={metrics.get('turnover_multiple', 0.0):.2f}x "
            f"net={metrics.get('net_profit', 0.0):.4f} "
            f"ret={metrics.get('total_return', 0.0) * 100:.4f}% "
            f"dd={metrics.get('max_drawdown', 0.0) * 100:.4f}%"
        )
        print(
            "   "
            f"trades={metrics.get('trade_count')} "
            f"funding={metrics.get('funding_pnl', 0.0):.4f} "
            f"fees={metrics.get('total_fees', 0.0):.4f} "
            f"avg_usage={metrics.get('avg_capital_usage', 0.0) * 100:.2f}%"
        )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    symbol = str(args.symbol).upper().strip()
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=float(args.days))
    report = build_short_volume_candidate_report(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=args.interval,
        total_buy_notional=float(args.budget),
        contract_type=args.contract_type,
        cache_dir=args.cache_dir,
        include_funding=True,
        fee_rate=0.0002,
        slippage=0.0,
        refresh=bool(args.refresh),
    )
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    report["symbol"] = symbol
    _print_report(report)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("")
        print(f"Saved JSON report to: {output_path}")


if __name__ == "__main__":
    main()
