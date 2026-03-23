from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .backtest import (
    supported_allocation_modes,
    supported_grid_level_modes,
    supported_strategy_directions,
)
from .data import load_or_fetch_candles, load_or_fetch_funding_rates, normalize_market_type
from .optimize import min_step_ratio_for_cost, optimize_grid_count


def _pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _float(value: float) -> str:
    return f"{value:,.4f}"


def _build_parser() -> argparse.ArgumentParser:
    supported_modes = ",".join(supported_allocation_modes())
    supported_grid_modes = ",".join(supported_grid_level_modes())
    parser = argparse.ArgumentParser(
        description="Directional grid optimizer for Binance futures or spot markets."
    )
    parser.add_argument(
        "--market-type",
        type=str,
        default="futures",
        choices=("futures", "spot"),
        help="Market type: futures / spot",
    )
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
    parser.add_argument(
        "--start-time",
        type=str,
        default="",
        help="UTC/ISO start time, e.g. 2025-01-01T00:00:00Z",
    )
    parser.add_argument(
        "--end-time",
        type=str,
        default="",
        help="UTC/ISO end time, e.g. 2026-01-01T00:00:00Z",
    )
    parser.add_argument("--interval", type=str, default="1h", help="Binance kline interval, e.g. 1h/4h")
    parser.add_argument("--n-min", type=int, default=5)
    parser.add_argument("--n-max", type=int, default=200)
    parser.add_argument("--fee-rate", type=float, default=0.0002, help="Per-side fee rate, default 0.0002")
    parser.add_argument("--slippage", type=float, default=0.0, help="Per-side slippage rate")
    parser.add_argument(
        "--include-funding",
        action="store_true",
        help="Include historical funding rates in PnL",
    )
    parser.add_argument("--funding-buffer", type=float, default=0.0, help="Conservative buffer in cost filter")
    parser.add_argument(
        "--allocation-modes",
        type=str,
        default="equal,linear",
        help=f"Comma-separated modes to optimize. Supported: {supported_modes}",
    )
    parser.add_argument(
        "--strategy-direction",
        type=str,
        default="long",
        choices=supported_strategy_directions(),
        help="Strategy direction: long / short / neutral",
    )
    parser.add_argument(
        "--grid-level-mode",
        type=str,
        default="arithmetic",
        choices=supported_grid_level_modes(),
        help=f"Grid price mode. Supported: {supported_grid_modes}",
    )
    parser.add_argument(
        "--objective",
        type=str,
        default="calmar",
        choices=[
            "calmar",
            "net_profit",
            "total_return",
            "annualized_return",
            "gross_trade_notional",
            "competition_volume",
        ],
        help="Optimization objective",
    )
    parser.add_argument(
        "--target-trade-volume",
        type=float,
        default=0.0,
        help="Optional target traded notional for competition_volume objective",
    )
    parser.add_argument(
        "--neutral-anchor-price",
        type=float,
        default=None,
        help="Optional neutral anchor price. Only used when strategy_direction=neutral.",
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
    print(f"Market type: {args.market_type}")
    print(f"Symbol: {args.symbol.upper()}")
    print(f"Range: {_float(args.min_price)} - {_float(args.max_price)}")
    print(f"Total buy notional: {_float(args.total_buy_notional)}")
    if args.start_time and args.end_time:
        print(f"Range time: {args.start_time} -> {args.end_time} / {args.interval}")
    else:
        print(f"Lookback: {args.lookback_days} days / {args.interval}")
    print(f"Fee rate (per side): {_pct(args.fee_rate)}")
    print(f"Slippage (per side): {_pct(args.slippage)}")
    print(f"Include funding: {'yes' if args.include_funding else 'no'}")
    print(f"Allocation modes: {args.allocation_modes}")
    print(f"Strategy direction: {args.strategy_direction}")
    print(f"Grid level mode: {args.grid_level_mode}")
    print(f"Objective: {args.objective}")
    if args.target_trade_volume > 0:
        print(f"Target trade volume: {_float(args.target_trade_volume)}")
    if args.strategy_direction == "neutral" and args.neutral_anchor_price is not None:
        print(f"Neutral anchor price: {_float(args.neutral_anchor_price)}")
    print(f"Min trade count filter: {args.min_trade_count}")
    print(f"Min avg capital usage filter: {_pct(args.min_avg_capital_usage)}")
    print("Trigger model: intrabar high/low path")
    print(f"Candle count: {candle_count}")
    print("")


def _print_top_results(top_results) -> None:
    print("=== Top Candidates (by objective) ===")
    print("N\tMode\tScore\tTurnoverX\tNetProfit\tAnnualized\tMaxDD\tTrades\tFees\tFunding")
    for item in top_results:
        score_str = f"{item.score:.4f}" if math.isfinite(item.score) else "INF"
        print(
            f"{item.n}\t{item.allocation_mode}\t{score_str}\t{item.turnover_multiple:.2f}x\t{_float(item.net_profit)}\t"
            f"{_pct(item.annualized_return)}\t{_pct(item.max_drawdown)}\t"
            f"{item.trade_count}\t{_float(item.total_fees)}\t{_float(item.funding_pnl)}"
        )
    print("")


def _print_best_detail(best, target_trade_volume: float = 0.0) -> None:
    print("=== Best Grid ===")
    print(f"Best N: {best.n}")
    print(f"Best allocation mode: {best.allocation_mode}")
    print(f"Strategy direction: {best.strategy_direction}")
    if best.strategy_direction == "neutral" and best.neutral_anchor_price is not None:
        print(f"Neutral anchor price: {_float(best.neutral_anchor_price)}")
    print(f"Objective score: {best.score:.4f}")
    print(f"Gross traded notional: {_float(best.gross_trade_notional)}")
    if target_trade_volume > 0:
        print(f"Target volume coverage: {_pct(best.gross_trade_notional / target_trade_volume)}")
    print(f"Turnover multiple: {best.turnover_multiple:.2f}x")
    print(f"Net profit: {_float(best.net_profit)}")
    print(f"Total return: {_pct(best.total_return)}")
    print(f"Annualized return: {_pct(best.annualized_return)}")
    print(f"Max drawdown: {_pct(best.max_drawdown)}")
    print(f"Total fees: {_float(best.total_fees)}")
    print(f"Funding pnl: {_float(best.funding_pnl)}")
    print(f"Funding events: {best.funding_event_count}")
    print(f"Trade count: {best.trade_count}")
    print(f"Win rate (closed grid legs): {_pct(best.win_rate)}")
    print(f"Avg capital usage: {_pct(best.avg_capital_usage)}")
    print(f"Max capital usage: {_pct(best.max_capital_usage)}")
    print(f"Period low/high: {_float(best.period_low)} / {_float(best.period_high)}")
    print(f"Period amplitude ((high-low)/low): {_pct(best.period_amplitude)}")
    print("")

    print("=== Per-grid Buy Plan ===")
    print("Idx\tSide\tBuyPrice\tSellPrice\tBuyNotional\tQty")
    n = best.n
    for i in range(n):
        buy_price = best.grid_levels[i]
        sell_price = best.grid_levels[i + 1]
        qty = best.per_grid_qty[i]
        notional = best.per_grid_notionals[i]
        side = best.grid_sides[i]
        print(
            f"{i + 1}\t{side}\t{_float(buy_price)}\t{_float(sell_price)}\t"
            f"{_float(notional)}\t{_float(qty)}"
        )


def _build_report_payload(args: argparse.Namespace, optimization, best, candle_count: int) -> dict:
    return {
        "symbol": args.symbol.upper(),
        "market_type": args.market_type,
        "input": {
            "min_price": args.min_price,
            "max_price": args.max_price,
            "total_buy_notional": args.total_buy_notional,
            "lookback_days": args.lookback_days,
            "start_time": args.start_time,
            "end_time": args.end_time,
            "interval": args.interval,
            "n_min": args.n_min,
            "n_max": args.n_max,
            "fee_rate": args.fee_rate,
            "slippage": args.slippage,
            "include_funding": args.include_funding,
            "funding_buffer": args.funding_buffer,
            "allocation_modes": [x.strip() for x in args.allocation_modes.split(",") if x.strip()],
            "strategy_direction": args.strategy_direction,
            "grid_level_mode": args.grid_level_mode,
            "objective": args.objective,
            "target_trade_volume": args.target_trade_volume,
            "neutral_anchor_price": args.neutral_anchor_price,
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
            "grid_level_mode": args.grid_level_mode,
            "strategy_direction": best.strategy_direction,
            "neutral_anchor_price": best.neutral_anchor_price,
            "score": best.score,
            "gross_trade_notional": best.gross_trade_notional,
            "target_trade_volume": args.target_trade_volume,
            "volume_coverage": (
                best.gross_trade_notional / args.target_trade_volume
                if args.target_trade_volume > 0
                else 1.0
            ),
            "turnover_multiple": best.turnover_multiple,
            "net_profit": best.net_profit,
            "total_return": best.total_return,
            "annualized_return": best.annualized_return,
            "max_drawdown": best.max_drawdown,
            "total_fees": best.total_fees,
            "funding_pnl": best.funding_pnl,
            "funding_event_count": best.funding_event_count,
            "trade_count": best.trade_count,
            "win_rate": best.win_rate,
            "avg_capital_usage": best.avg_capital_usage,
            "max_capital_usage": best.max_capital_usage,
            "period_low": best.period_low,
            "period_high": best.period_high,
            "period_amplitude": best.period_amplitude,
            "per_grid_notionals": best.per_grid_notionals,
            "grid_levels": best.grid_levels,
            "grid_sides": best.grid_sides,
            "per_grid_qty": best.per_grid_qty,
        },
        "top": [
            {
                "n": item.n,
                "allocation_mode": item.allocation_mode,
                "grid_level_mode": args.grid_level_mode,
                "strategy_direction": item.strategy_direction,
                "score": item.score,
                "gross_trade_notional": item.gross_trade_notional,
                "target_trade_volume": args.target_trade_volume,
                "volume_coverage": (
                    item.gross_trade_notional / args.target_trade_volume
                    if args.target_trade_volume > 0
                    else 1.0
                ),
                "turnover_multiple": item.turnover_multiple,
                "net_profit": item.net_profit,
                "annualized_return": item.annualized_return,
                "max_drawdown": item.max_drawdown,
                "total_fees": item.total_fees,
                "funding_pnl": item.funding_pnl,
                "funding_event_count": item.funding_event_count,
                "trade_count": item.trade_count,
                "period_amplitude": item.period_amplitude,
            }
            for item in optimization.top_results
        ],
    }


def _write_plan_csv(path: Path, best) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write("idx,grid_side,buy_price,sell_price,buy_notional,qty\n")
        for i in range(best.n):
            f.write(
                f"{i + 1},"
                f"{best.grid_sides[i]},"
                f"{best.grid_levels[i]:.8f},"
                f"{best.grid_levels[i + 1]:.8f},"
                f"{best.per_grid_notionals[i]:.8f},"
                f"{best.per_grid_qty[i]:.8f}\n"
            )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.market_type = normalize_market_type(args.market_type)

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
    if args.target_trade_volume < 0:
        raise SystemExit("--target-trade-volume must be >= 0")
    if args.neutral_anchor_price is not None and args.neutral_anchor_price <= 0:
        raise SystemExit("--neutral-anchor-price must be > 0")
    if args.market_type == "spot":
        if args.include_funding:
            raise SystemExit("--include-funding is not supported for spot")
        if args.strategy_direction != "long":
            raise SystemExit("--strategy-direction must be long when --market-type=spot")
    start_time = None
    end_time = None
    if bool(args.start_time) != bool(args.end_time):
        raise SystemExit("--start-time and --end-time must be provided together")
    if args.start_time and args.end_time:
        try:
            start_time = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
            end_time = datetime.fromisoformat(args.end_time.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SystemExit(f"Invalid --start-time/--end-time: {exc}") from exc
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        else:
            start_time = start_time.astimezone(timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        else:
            end_time = end_time.astimezone(timezone.utc)
        if start_time >= end_time:
            raise SystemExit("--start-time must be earlier than --end-time")
    allocation_modes = [x.strip().lower() for x in args.allocation_modes.split(",") if x.strip()]
    if not allocation_modes:
        raise SystemExit("allocation modes cannot be empty")
    supported_set = set(supported_allocation_modes())
    unknown = [x for x in allocation_modes if x not in supported_set]
    if unknown:
        raise SystemExit(
            f"unsupported allocation modes: {','.join(unknown)}; "
            f"supported: {','.join(sorted(supported_set))}"
        )

    candles = load_or_fetch_candles(
        symbol=args.symbol,
        interval=args.interval,
        lookback_days=args.lookback_days,
        start_time=start_time,
        end_time=end_time,
        cache_dir=args.cache_dir,
        market_type=args.market_type,
        refresh=args.refresh,
    )
    funding_rates = []
    if args.include_funding:
        if start_time is None or end_time is None:
            end_dt = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=args.lookback_days)
        else:
            start_dt = start_time
            end_dt = end_time
        funding_rates = load_or_fetch_funding_rates(
            symbol=args.symbol,
            start_time=start_dt,
            end_time=end_dt,
            cache_dir=args.cache_dir,
            market_type=args.market_type,
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
        grid_level_mode=args.grid_level_mode,
        strategy_direction=args.strategy_direction,
        fee_rate=args.fee_rate,
        slippage=args.slippage,
        funding_buffer=args.funding_buffer,
        allocation_modes=allocation_modes,
        objective=args.objective,
        target_trade_volume=args.target_trade_volume,
        min_trade_count=args.min_trade_count,
        min_avg_capital_usage=args.min_avg_capital_usage,
        neutral_anchor_price=args.neutral_anchor_price,
        top_k=args.top_k,
        funding_rates=funding_rates,
        bootstrap_positions=args.market_type != "spot",
    )

    if optimization.best is None:
        print("No valid grid candidate after current filters.")
        print("Try lowering constraints, widening range, or reducing n_max.")
        return

    best = optimization.best

    _print_top_results(optimization.top_results)
    _print_best_detail(best, target_trade_volume=args.target_trade_volume)

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
