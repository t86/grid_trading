from __future__ import annotations

import math
from collections.abc import Callable

from .backtest import run_backtest
from .types import Candle, OptimizationResult


def min_step_ratio_for_cost(fee_rate: float, slippage: float = 0.0, funding_buffer: float = 0.0) -> float:
    return 2 * fee_rate + 2 * slippage + funding_buffer


def objective_value(result, objective: str) -> float:
    mode = objective.strip().lower()
    if mode == "calmar":
        if math.isfinite(result.calmar):
            return result.calmar
        return 1e12 if result.annualized_return > 0 else -1e12
    if mode == "net_profit":
        return result.net_profit
    if mode == "total_return":
        return result.total_return
    if mode == "annualized_return":
        return result.annualized_return
    raise ValueError(
        "Unsupported objective. Use one of: calmar, net_profit, total_return, annualized_return"
    )


def optimize_grid_count(
    candles: list[Candle],
    min_price: float,
    max_price: float,
    total_buy_notional: float,
    n_min: int,
    n_max: int,
    fee_rate: float = 0.0002,
    slippage: float = 0.0,
    funding_buffer: float = 0.0,
    allocation_modes: list[str] | None = None,
    objective: str = "calmar",
    min_trade_count: int = 0,
    min_avg_capital_usage: float = 0.0,
    top_k: int = 5,
    progress_callback: Callable[[dict], None] | None = None,
) -> OptimizationResult:
    if n_min <= 0 or n_max <= 0:
        raise ValueError("n_min and n_max must be > 0")
    if n_min > n_max:
        raise ValueError("n_min must be <= n_max")
    if min_trade_count < 0:
        raise ValueError("min_trade_count must be >= 0")
    if min_avg_capital_usage < 0:
        raise ValueError("min_avg_capital_usage must be >= 0")

    mid_price = (min_price + max_price) / 2.0
    min_ratio = min_step_ratio_for_cost(
        fee_rate=fee_rate, slippage=slippage, funding_buffer=funding_buffer
    )
    modes = allocation_modes if allocation_modes else ["equal", "linear"]
    normalized_modes: list[str] = []
    for mode in modes:
        cleaned = mode.strip().lower()
        if cleaned and cleaned not in normalized_modes:
            normalized_modes.append(cleaned)
    if not normalized_modes:
        raise ValueError("allocation_modes must contain at least one mode")

    tested = 0
    skipped_by_cost = 0
    candidates = []
    total_candidates = (n_max - n_min + 1) * len(normalized_modes)
    processed = 0
    if progress_callback:
        progress_callback(
            {
                "processed": 0,
                "total": total_candidates,
                "n": None,
                "mode": None,
                "status": "started",
            }
        )

    for n in range(n_min, n_max + 1):
        step = (max_price - min_price) / n
        step_ratio = step / mid_price
        if step_ratio <= min_ratio:
            skipped_by_cost += 1
            processed += len(normalized_modes)
            if progress_callback:
                progress_callback(
                    {
                        "processed": processed,
                        "total": total_candidates,
                        "n": n,
                        "mode": None,
                        "status": "skipped_cost",
                    }
                )
            continue

        for mode in normalized_modes:
            tested += 1
            processed += 1
            result = run_backtest(
                candles=candles,
                min_price=min_price,
                max_price=max_price,
                n=n,
                total_buy_notional=total_buy_notional,
                allocation_mode=mode,
                fee_rate=fee_rate,
                slippage=slippage,
                capture_trades=False,
            )
            if result.trade_count < min_trade_count:
                if progress_callback:
                    progress_callback(
                        {
                            "processed": processed,
                            "total": total_candidates,
                            "n": n,
                            "mode": mode,
                            "status": "filtered",
                        }
                    )
                continue
            if result.avg_capital_usage < min_avg_capital_usage:
                if progress_callback:
                    progress_callback(
                        {
                            "processed": processed,
                            "total": total_candidates,
                            "n": n,
                            "mode": mode,
                            "status": "filtered",
                        }
                    )
                continue
            result.score = objective_value(result, objective)
            candidates.append(result)
            if progress_callback:
                progress_callback(
                    {
                        "processed": processed,
                        "total": total_candidates,
                        "n": n,
                        "mode": mode,
                        "status": "tested",
                    }
                )

    if not candidates:
        return OptimizationResult(best=None, top_results=[], skipped_by_cost=skipped_by_cost, tested=tested)

    def _sort_key(item) -> tuple[float, float]:
        score = item.score if math.isfinite(item.score) else -1e12
        return (score, item.net_profit)

    sorted_results = sorted(candidates, key=_sort_key, reverse=True)
    top_results = sorted_results[: max(1, top_k)]
    best = top_results[0]

    return OptimizationResult(
        best=best,
        top_results=top_results,
        skipped_by_cost=skipped_by_cost,
        tested=tested,
    )
