from __future__ import annotations

import math
from statistics import mean

from .types import BacktestResult, Candle, Trade


def build_grid_levels(min_price: float, max_price: float, n: int) -> list[float]:
    if n <= 0:
        raise ValueError("n must be > 0")
    if min_price <= 0 or max_price <= 0:
        raise ValueError("price bounds must be > 0")
    if min_price >= max_price:
        raise ValueError("min_price must be < max_price")
    step = (max_price - min_price) / n
    return [min_price + i * step for i in range(n + 1)]


def _max_drawdown(nav: list[float]) -> float:
    if not nav:
        return 0.0
    peak = nav[0]
    max_dd = 0.0
    for value in nav:
        if value > peak:
            peak = value
        if peak <= 0:
            continue
        dd = (peak - value) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd


def build_per_grid_notionals(
    total_buy_notional: float,
    n: int,
    allocation_mode: str,
) -> list[float]:
    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")
    if n <= 0:
        raise ValueError("n must be > 0")

    mode = allocation_mode.lower().strip()
    if mode == "equal":
        return [total_buy_notional / n] * n

    if mode in {"linear", "linear_increase"}:
        # For long grids, lower price levels get larger allocation.
        weights = [n - i for i in range(n)]
        total_weight = n * (n + 1) / 2
        return [total_buy_notional * (weight / total_weight) for weight in weights]

    raise ValueError(f"Unsupported allocation_mode: {allocation_mode}")


def _intrabar_path(candle: Candle) -> list[float]:
    if candle.close >= candle.open:
        points = [candle.open, candle.low, candle.high, candle.close]
    else:
        points = [candle.open, candle.high, candle.low, candle.close]
    compact: list[float] = [points[0]]
    for point in points[1:]:
        if point != compact[-1]:
            compact.append(point)
    return compact


def run_backtest(
    candles: list[Candle],
    min_price: float,
    max_price: float,
    n: int,
    total_buy_notional: float,
    allocation_mode: str = "equal",
    fee_rate: float = 0.0002,
    slippage: float = 0.0,
    capture_trades: bool = False,
) -> BacktestResult:
    if len(candles) < 1:
        raise ValueError("Need at least 1 candle for backtest")
    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")

    levels = build_grid_levels(min_price=min_price, max_price=max_price, n=n)
    per_grid_notionals = build_per_grid_notionals(
        total_buy_notional=total_buy_notional, n=n, allocation_mode=allocation_mode
    )
    per_grid_qty = [per_grid_notionals[i] / levels[i] for i in range(n)]

    open_qty = [0.0] * n
    entry_price = [0.0] * n

    realized_pnl = 0.0
    total_fees = 0.0
    wins = 0
    closed_trades = 0
    fill_count = 0
    capital_usage_series: list[float] = []
    equity_series: list[float] = []
    trades: list[Trade] | None = [] if capture_trades else None

    def _record_buy(i: int, fill_price: float, ts) -> None:
        nonlocal total_fees, fill_count
        qty = per_grid_qty[i]
        notional = fill_price * qty
        fee = notional * fee_rate

        open_qty[i] = qty
        entry_price[i] = fill_price
        total_fees += fee
        fill_count += 1

        if trades is not None:
            trades.append(
                Trade(
                    ts=ts,
                    side="BUY",
                    grid_index=i,
                    price=fill_price,
                    qty=qty,
                    notional=notional,
                    fee=fee,
                )
            )

    def _record_sell(i: int, fill_price: float, ts) -> None:
        nonlocal realized_pnl, total_fees, closed_trades, wins, fill_count
        qty = open_qty[i]
        notional = fill_price * qty
        fee = notional * fee_rate
        pnl = (fill_price - entry_price[i]) * qty

        realized_pnl += pnl
        total_fees += fee
        closed_trades += 1
        if pnl > 0:
            wins += 1
        fill_count += 1

        if trades is not None:
            trades.append(
                Trade(
                    ts=ts,
                    side="SELL",
                    grid_index=i,
                    price=fill_price,
                    qty=qty,
                    notional=notional,
                    fee=fee,
                )
            )

        open_qty[i] = 0.0
        entry_price[i] = 0.0

    def _process_segment(start_price: float, end_price: float, ts) -> None:
        if end_price < start_price:
            for i in range(n - 1, -1, -1):
                buy_level = levels[i]
                if end_price <= buy_level < start_price and open_qty[i] == 0.0:
                    _record_buy(i=i, fill_price=buy_level * (1.0 + slippage), ts=ts)
        elif end_price > start_price:
            for i in range(n):
                sell_level = levels[i + 1]
                if start_price < sell_level <= end_price and open_qty[i] > 0.0:
                    _record_sell(i=i, fill_price=sell_level * (1.0 - slippage), ts=ts)

    for candle in candles:
        path = _intrabar_path(candle)
        for idx in range(len(path) - 1):
            _process_segment(path[idx], path[idx + 1], candle.close_time)

        mark_price = candle.close

        open_notional = 0.0
        unrealized = 0.0
        for i in range(n):
            if open_qty[i] > 0.0:
                open_notional += open_qty[i] * entry_price[i]
                unrealized += (mark_price - entry_price[i]) * open_qty[i]

        capital_usage_series.append(open_notional)
        equity_series.append(realized_pnl + unrealized - total_fees)

    final_price = candles[-1].close
    unrealized_end = 0.0
    for i in range(n):
        if open_qty[i] > 0.0:
            unrealized_end += (final_price - entry_price[i]) * open_qty[i]

    net_profit = realized_pnl + unrealized_end - total_fees
    total_return = net_profit / total_buy_notional

    backtest_days = (candles[-1].close_time - candles[0].open_time).total_seconds() / 86_400
    if backtest_days > 0 and total_return > -1.0:
        annualized = (1.0 + total_return) ** (365.0 / backtest_days) - 1.0
    else:
        annualized = -1.0 if total_return <= -1.0 else 0.0

    nav = [total_buy_notional + x for x in equity_series]
    max_drawdown = _max_drawdown(nav)

    if max_drawdown > 0:
        calmar = annualized / max_drawdown
    else:
        calmar = math.inf if annualized > 0 else 0.0

    score = calmar if math.isfinite(calmar) else 1e12
    win_rate = wins / closed_trades if closed_trades > 0 else 0.0
    avg_capital_usage = (
        mean(capital_usage_series) / total_buy_notional if capital_usage_series else 0.0
    )
    max_capital_usage = (
        max(capital_usage_series) / total_buy_notional if capital_usage_series else 0.0
    )

    return BacktestResult(
        n=n,
        allocation_mode=allocation_mode,
        grid_levels=levels,
        per_grid_notionals=per_grid_notionals,
        per_grid_qty=per_grid_qty,
        net_profit=net_profit,
        total_fees=total_fees,
        total_return=total_return,
        annualized_return=annualized,
        max_drawdown=max_drawdown,
        calmar=calmar,
        score=score,
        trade_count=fill_count,
        win_rate=win_rate,
        avg_capital_usage=avg_capital_usage,
        max_capital_usage=max_capital_usage,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_end,
        trades=trades,
    )
