from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from .data import load_or_fetch_candles
from .optimize import min_step_ratio_for_cost, optimize_grid_count


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Arithmetic long-grid optimizer for BTC/ETH perpetuals.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="e.g., BTCUSDT or ETHUSDT")
    parser.add_argument("--min-price", type=float, required=True, help="Grid lower bound")
    parser.add_argument("--max-price", type=float, required=True, help="Grid upper bound")
    parser.add_argument(
        "--total-buy-notional",
        type=float,
        required=True,
        help="Total notional to be fully deployed when price reaches min-price",
    )
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--interval", type=str, default="1h", help="Binance kline interval, e.g. 1h/4h")
    parser.add_argument("--n-min", type=int, default=5)
    parser.add_argument("--n-max", type=int, default=200)
    parser.add_argument("--fee-rate", type=float, default=0.0002, help="Per-side fee rate, default 0.0002")
    parser.add_argument("--slippage", type=float, default=0.0, help="Per-side slippage rate")
    parser.add_argument("--funding-buffer", type=float, default=0.0, help="Conservative buffer in cost filter")
    parser.add_argument(
        "--allocation-modes",
        type=str,
        default="equal,linear",
        help="Comma-separated modes to optimize, e.g. equal,linear",
    )
    parser.add_argument(
        "--objective",
        type=str,
        default="calmar",
        choices=["calmar", "net_profit", "total_return", "annualized_return"],
        help="Optimization objective",
    )
    parser.add_argument(
        "--min-trade-count",
        type=int,
        default=0,
        help="Filter out candidates below this trade count",
    )
    parser.add_argument(
        "--min-avg-capital-usage",
        type=float,
        default=0.0,
        help="Filter out candidates below this average capital usage ratio (0-1)",
    )
    parser.add_argument("--cache-dir", type=str, default="data")
    parser.add_argument("--refresh", action="store_true", help="Force re-download klines")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--report-json", type=str, default="", help="Optional output json path")
    parser.add_argument("--plan-csv", type=str, default="", help="Optional per-grid plan csv path")
    return parser


def _print_header(args: argparse.Namespace, candle_count: int) -> None:
    print("=== Input ===")
    print(f"Symbol: {args.symbol.upper()}")
    print(f"Range: {_float(args.min_price)} - {_float(args.max_price)}")
    print(f"Total buy notional: {_float(args.total_buy_notional)}")
    print(f"Lookback: {args.lookback_days} days / {args.interval}")
    print(f"Fee rate (per side): {_pct(args.fee_rate)}")
    print(f"Slippage (per side): {_pct(args.slippage)}")
    print(f"Allocation modes: {args.allocation_modes}")
    print(f"Objective: {args.objective}")
    print(f"Min trade count filter: {args.min_trade_count}")
    print(f"Min avg capital usage filter: {_pct(args.min_avg_capital_usage)}")
    print("Trigger model: intrabar high/low path")
    print(f"Candle count: {candle_count}")
    print("")


def _print_top_results(top_results) -> None:
    print("=== Top Candidates (by objective) ===")
    print("N\tMode\tScore\tNetProfit\tAnnualized\tMaxDD\tTrades\tFees")
    for item in top_results:
        score_str = f"{item.score:.4f}" if math.isfinite(item.score) else "INF"
        print(
            f"{item.n}\t{item.allocation_mode}\t{score_str}\t{_float(item.net_profit)}\t"
            f"{_pct(item.annualized_return)}\t{_pct(item.max_drawdown)}\t"
            f"{item.trade_count}\t{_float(item.total_fees)}"
        )
    print("")


def _print_best_detail(best) -> None:
    print("=== Best Grid ===")
    print(f"Best N: {best.n}")
    print(f"Best allocation mode: {best.allocation_mode}")
    print(f"Objective score: {best.score:.4f}")
    print(f"Net profit: {_float(best.net_profit)}")
    print(f"Total return: {_pct(best.total_return)}")
    print(f"Annualized return: {_pct(best.annualized_return)}")
    print(f"Max drawdown: {_pct(best.max_drawdown)}")
    print(f"Total fees: {_float(best.total_fees)}")
    print(f"Trade count: {best.trade_count}")
    print(f"Win rate (closed grid legs): {_pct(best.win_rate)}")
    print(f"Avg capital usage: {_pct(best.avg_capital_usage)}")
    print(f"Max capital usage: {_pct(best.max_capital_usage)}")
    print("")

    print("=== Per-grid Buy Plan ===")
    print("Idx\tBuyPrice\tSellPrice\tBuyNotional\tQty")
    n = best.n
    for i in range(n):
        buy_price = best.grid_levels[i]
        sell_price = best.grid_levels[i + 1]
        qty = best.per_grid_qty[i]
        notional = best.per_grid_notionals[i]
        print(
            f"{i + 1}\t{_float(buy_price)}\t{_float(sell_price)}\t"
            f"{_float(notional)}\t{_float(qty)}"
        )


def _build_report_payload(args: argparse.Namespace, optimization, best, candle_count: int) -> dict:
    return {
        "symbol": args.symbol.upper(),
        "input": {
            "min_price": args.min_price,
            "max_price": args.max_price,
            "total_buy_notional": args.total_buy_notional,
            "lookback_days": args.lookback_days,
            "interval": args.interval,
            "n_min": args.n_min,
            "n_max": args.n_max,
            "fee_rate": args.fee_rate,
            "slippage": args.slippage,
            "funding_buffer": args.funding_buffer,
            "allocation_modes": [x.strip() for x in args.allocation_modes.split(",") if x.strip()],
            "objective": args.objective,
            "min_trade_count": args.min_trade_count,
            "min_avg_capital_usage": args.min_avg_capital_usage,
        },
        "data": {"candles": candle_count},
        "search": {
            "tested": optimization.tested,
            "skipped_by_cost": optimization.skipped_by_cost,
            "min_step_ratio_for_cost": min_step_ratio_for_cost(
                fee_rate=args.fee_rate,
                slippage=args.slippage,
                funding_buffer=args.funding_buffer,
            ),
        },
        "best": {
            "n": best.n,
            "allocation_mode": best.allocation_mode,
            "score": best.score,
            "net_profit": best.net_profit,
            "total_return": best.total_return,
            "annualized_return": best.annualized_return,
            "max_drawdown": best.max_drawdown,
            "total_fees": best.total_fees,
            "trade_count": best.trade_count,
            "win_rate": best.win_rate,
            "avg_capital_usage": best.avg_capital_usage,
            "max_capital_usage": best.max_capital_usage,
            "per_grid_notionals": best.per_grid_notionals,
            "grid_levels": best.grid_levels,
            "per_grid_qty": best.per_grid_qty,
        },
        "top": [
            {
                "n": item.n,
                "allocation_mode": item.allocation_mode,
                "score": item.score,
                "net_profit": item.net_profit,
                "annualized_return": item.annualized_return,
                "max_drawdown": item.max_drawdown,
                "total_fees": item.total_fees,
                "trade_count": item.trade_count,
            }
            for item in optimization.top_results
        ],
    }


def _write_plan_csv(path: Path, best) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("idx,buy_price,sell_price,buy_notional,qty\n")
        for i in range(best.n):
            f.write(
                f"{i + 1},"
                f"{best.grid_levels[i]:.8f},"
                f"{best.grid_levels[i + 1]:.8f},"
                f"{best.per_grid_notionals[i]:.8f},"
                f"{best.per_grid_qty[i]:.8f}\n"
            )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.min_price >= args.max_price:
        raise SystemExit("--min-price must be less than --max-price")
    if args.total_buy_notional <= 0:
        raise SystemExit("--total-buy-notional must be > 0")
    if args.n_min <= 0 or args.n_max <= 0 or args.n_min > args.n_max:
        raise SystemExit("invalid n range, require 0 < n_min <= n_max")
    if args.min_trade_count < 0:
        raise SystemExit("--min-trade-count must be >= 0")
    if args.min_avg_capital_usage < 0 or args.min_avg_capital_usage > 1:
        raise SystemExit("--min-avg-capital-usage must be in [0, 1]")
    allocation_modes = [x.strip().lower() for x in args.allocation_modes.split(",") if x.strip()]
    if not allocation_modes:
        raise SystemExit("allocation modes cannot be empty")

    candles = load_or_fetch_candles(
        symbol=args.symbol,
        interval=args.interval,
        lookback_days=args.lookback_days,
        cache_dir=args.cache_dir,
        refresh=args.refresh,
    )
    _print_header(args=args, candle_count=len(candles))

    optimization = optimize_grid_count(
        candles=candles,
        min_price=args.min_price,
        max_price=args.max_price,
        total_buy_notional=args.total_buy_notional,
        n_min=args.n_min,
        n_max=args.n_max,
        fee_rate=args.fee_rate,
        slippage=args.slippage,
        funding_buffer=args.funding_buffer,
        allocation_modes=allocation_modes,
        objective=args.objective,
        min_trade_count=args.min_trade_count,
        min_avg_capital_usage=args.min_avg_capital_usage,
        top_k=args.top_k,
    )

    if optimization.best is None:
        print("No valid grid candidate after current filters.")
        print("Try lowering constraints, widening range, or reducing n_max.")
        return

    best = optimization.best

    _print_top_results(optimization.top_results)
    _print_best_detail(best)

    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        payload = _build_report_payload(args, optimization, best, len(candles))
        report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("")
        print(f"JSON report saved: {report_path}")

    if args.plan_csv:
        plan_path = Path(args.plan_csv)
        _write_plan_csv(plan_path, best)
        print(f"Grid plan csv saved: {plan_path}")


if __name__ == "__main__":
    main()
