from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .competition import (
    build_competition_symbol_report,
    competition_profile_keys,
    competition_symbols,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run fixed competition-strategy backtests for supported Binance futures symbols."
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=",".join(competition_symbols()),
        help="Comma-separated competition symbols",
    )
    parser.add_argument(
        "--window-days",
        type=str,
        default="3",
        help="Comma-separated lookback windows in days, e.g. 3 or 15,7,3",
    )
    parser.add_argument(
        "--profiles",
        type=str,
        default=",".join(competition_profile_keys()),
        help="Comma-separated profile keys",
    )
    parser.add_argument("--interval", type=str, default="1m")
    parser.add_argument("--cache-dir", type=str, default="data")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--report-json", type=str, default="")
    return parser


def _parse_csv(raw: str) -> list[str]:
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def _parse_window_days(raw: str) -> list[int]:
    windows: list[int] = []
    for token in _parse_csv(raw):
        try:
            value = int(token)
        except ValueError as exc:
            raise SystemExit(f"Invalid --window-days entry: {token}") from exc
        if value <= 0:
            raise SystemExit("--window-days must be positive integers")
        windows.append(value)
    if not windows:
        raise SystemExit("--window-days cannot be empty")
    return windows


def _print_report(report: dict) -> None:
    print(f"=== {report['symbol']} ===")
    print(f"Generated at (UTC): {report['generated_at_utc']}")
    print(f"Last close: {report['market_snapshot']['last_close']:.8f}")
    for window_key, window_range in report["market_snapshot"]["ranges"].items():
        print(f"{window_key} range: {window_range['low']:.8f} - {window_range['high']:.8f}")
    for profile_key, payload in report["strategies"].items():
        print(
            f"[{profile_key}] range={payload['min_price']:.8f}-{payload['max_price']:.8f} "
            f"n={payload['n']} mode={payload['allocation_mode']}"
        )
        for window_key, metrics in payload["window_metrics"].items():
            print(
                f"  {window_key}: turnover={metrics['turnover_multiple']:.2f}x "
                f"net={metrics['net_profit']:.4f} trades={metrics['trade_count']} "
                f"fees={metrics['total_fees']:.4f} funding={metrics['funding_pnl']:.4f} "
                f"max_dd={metrics['max_drawdown'] * 100:.2f}%"
            )
    print("")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    symbols = [item.upper() for item in _parse_csv(args.symbols)]
    profiles = [item.lower() for item in _parse_csv(args.profiles)]
    windows = _parse_window_days(args.window_days)
    end_time = datetime.now(timezone.utc)

    reports = [
        build_competition_symbol_report(
            symbol=symbol,
            window_days=windows,
            profile_keys=profiles,
            interval=args.interval,
            cache_dir=args.cache_dir,
            refresh=args.refresh,
            end_time=end_time,
        )
        for symbol in symbols
    ]

    for report in reports:
        _print_report(report)

    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at_utc": end_time.isoformat() if reports else None,
            "symbols": symbols,
            "profiles": profiles,
            "window_days": windows,
            "reports": reports,
        }
        report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"JSON report saved: {report_path}")


if __name__ == "__main__":
    main()
