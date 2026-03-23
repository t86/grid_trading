from __future__ import annotations

import math
from statistics import mean

from .types import BacktestResult, Candle, FundingEvent, FundingRate, Trade

GEOMETRIC_RATIO = 1.15

ALLOCATION_MODE_DESCRIPTIONS: dict[str, str] = {
    "equal": "每格等额分配",
    "equal_qty": "每格等数量（名义随价格变化）",
    "linear": "越低价越多（线性递增）",
    "linear_reverse": "越高价越多（线性反向）",
    "quadratic": "越低价越多（平方递增）",
    "quadratic_reverse": "越高价越多（平方反向）",
    "geometric": "越低价越多（几何递增）",
    "geometric_reverse": "越高价越多（几何反向）",
    "center_heavy": "中间格子更多，两端更少",
    "edge_heavy": "两端格子更多，中间更少",
}

GRID_LEVEL_MODE_DESCRIPTIONS: dict[str, str] = {
    "arithmetic": "等差（固定价差）",
    "geometric": "等比（固定百分比）",
}


def allocation_mode_descriptions() -> dict[str, str]:
    return dict(ALLOCATION_MODE_DESCRIPTIONS)


def supported_allocation_modes() -> list[str]:
    return list(ALLOCATION_MODE_DESCRIPTIONS.keys())


def grid_level_mode_descriptions() -> dict[str, str]:
    return dict(GRID_LEVEL_MODE_DESCRIPTIONS)


def supported_grid_level_modes() -> list[str]:
    return list(GRID_LEVEL_MODE_DESCRIPTIONS.keys())


def supported_strategy_directions() -> list[str]:
    return ["long", "short", "neutral"]


def _normalize_grid_level_mode(grid_level_mode: str) -> str:
    mode = grid_level_mode.strip().lower()
    if mode not in set(supported_grid_level_modes()):
        raise ValueError(
            f"Unsupported grid_level_mode: {grid_level_mode}. "
            f"Supported: {', '.join(supported_grid_level_modes())}"
        )
    return mode


def build_grid_levels(
    min_price: float,
    max_price: float,
    n: int,
    grid_level_mode: str = "arithmetic",
) -> list[float]:
    if n <= 0:
        raise ValueError("n must be > 0")
    if min_price <= 0 or max_price <= 0:
        raise ValueError("price bounds must be > 0")
    if min_price >= max_price:
        raise ValueError("min_price must be < max_price")
    mode = _normalize_grid_level_mode(grid_level_mode)
    if mode == "arithmetic":
        step = (max_price - min_price) / n
        return [min_price + i * step for i in range(n + 1)]

    ratio = (max_price / min_price) ** (1.0 / n)
    if ratio <= 1.0:
        raise ValueError("geometric ratio must be > 1")
    levels = [min_price * (ratio**i) for i in range(n + 1)]
    levels[0] = min_price
    levels[-1] = max_price
    return levels


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
    price_levels: list[float] | None = None,
) -> list[float]:
    if total_buy_notional <= 0:
        raise ValueError("total_buy_notional must be > 0")
    if n <= 0:
        raise ValueError("n must be > 0")

    mode = allocation_mode.lower().strip()
    center = (n - 1) / 2.0
    weights: list[float]

    if mode == "equal":
        weights = [1.0] * n
    elif mode == "equal_qty":
        if price_levels is None or len(price_levels) != n + 1:
            raise ValueError("equal_qty mode requires price_levels with length n+1")
        # Equal base-asset quantity => per-grid notional proportional to price.
        weights = [float(price_levels[i]) for i in range(n)]
    elif mode in {"linear", "linear_increase"}:
        # For long grids, lower price levels get larger allocation.
        weights = [float(n - i) for i in range(n)]
    elif mode == "linear_reverse":
        weights = [float(i + 1) for i in range(n)]
    elif mode == "quadratic":
        weights = [float((n - i) ** 2) for i in range(n)]
    elif mode == "quadratic_reverse":
        weights = [float((i + 1) ** 2) for i in range(n)]
    elif mode == "geometric":
        weights = [GEOMETRIC_RATIO ** (n - i - 1) for i in range(n)]
    elif mode == "geometric_reverse":
        weights = [GEOMETRIC_RATIO ** i for i in range(n)]
    elif mode == "center_heavy":
        weights = [1.0 / (abs(i - center) + 1.0) for i in range(n)]
    elif mode == "edge_heavy":
        weights = [abs(i - center) + 1.0 for i in range(n)]
    else:
        raise ValueError(
            f"Unsupported allocation_mode: {allocation_mode}. "
            f"Supported: {', '.join(supported_allocation_modes())}"
        )

    total_weight = sum(weights)
    if total_weight <= 0:
        raise ValueError(f"invalid weights for allocation_mode: {allocation_mode}")
    return [total_buy_notional * (weight / total_weight) for weight in weights]


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


def _normalize_strategy_direction(direction: str) -> str:
    normalized = direction.strip().lower()
    if normalized not in {"long", "short", "neutral"}:
        raise ValueError(
            f"Unsupported strategy_direction: {direction}. "
            f"Supported: {', '.join(supported_strategy_directions())}"
        )
    return normalized


def _build_grid_sides(levels: list[float], strategy_direction: str, anchor_price: float | None) -> list[str]:
    n = len(levels) - 1
    if strategy_direction == "long":
        return ["long"] * n
    if strategy_direction == "short":
        return ["short"] * n
    if anchor_price is None:
        raise ValueError("neutral strategy requires anchor_price")
    sides: list[str] = []
    for i in range(n):
        mid = (levels[i] + levels[i + 1]) / 2.0
        sides.append("long" if mid < anchor_price else "short")
    return sides


def _entry_reference_price(levels: list[float], grid_index: int, grid_side: str) -> float:
    if grid_side == "long":
        return levels[grid_index]
    if grid_side == "short":
        return levels[grid_index + 1]
    raise ValueError(f"Unsupported grid_side: {grid_side}")


def run_backtest(
    candles: list[Candle],
    min_price: float,
    max_price: float,
    n: int,
    total_buy_notional: float,
    grid_level_mode: str = "arithmetic",
    allocation_mode: str = "equal",
    strategy_direction: str = "long",
    fee_rate: float = 0.0002,
    slippage: float = 0.0,
    funding_rates: list[FundingRate] | None = None,
    fixed_per_grid_qty: float | None = None,
    neutral_anchor_price: float | None = None,
    bootstrap_positions: bool = True,
    capture_funding_events: bool = False,
    capture_trades: bool = False,
    capture_curves: bool = False,
) -> BacktestResult:
    if len(candles) < 1:
        raise ValueError("Need at least 1 candle for backtest")

    levels = build_grid_levels(
        min_price=min_price,
        max_price=max_price,
        n=n,
        grid_level_mode=grid_level_mode,
    )
    normalized_direction = _normalize_strategy_direction(strategy_direction)
    if normalized_direction == "neutral":
        anchor_price = float(neutral_anchor_price) if neutral_anchor_price is not None else float(candles[0].open)
        if anchor_price <= 0:
            raise ValueError("neutral_anchor_price must be > 0")
    else:
        anchor_price = None
    grid_sides = _build_grid_sides(levels, normalized_direction, anchor_price)
    entry_ref_prices = [_entry_reference_price(levels, i, grid_sides[i]) for i in range(n)]

    if fixed_per_grid_qty is not None:
        if fixed_per_grid_qty <= 0:
            raise ValueError("fixed_per_grid_qty must be > 0")
        per_grid_qty = [fixed_per_grid_qty for _ in range(n)]
        per_grid_notionals = [
            fixed_per_grid_qty * entry_ref_prices[i] for i in range(n)
        ]
        capital_base_notional = sum(per_grid_notionals)
    else:
        if total_buy_notional <= 0:
            raise ValueError("total_buy_notional must be > 0")
        raw_notionals = build_per_grid_notionals(
            total_buy_notional=total_buy_notional,
            n=n,
            allocation_mode=allocation_mode,
            price_levels=levels,
        )
        raw_qty = [raw_notionals[i] / entry_ref_prices[i] for i in range(n)]
        per_grid_qty = list(raw_qty)
        per_grid_notionals = list(raw_notionals)
        capital_base_notional = float(total_buy_notional)
        if normalized_direction in {"long", "short"}:
            target_price = min_price if normalized_direction == "long" else max_price
            total_qty_raw = sum(raw_qty)
            if target_price > 0 and total_qty_raw > 0:
                # Interpret budget as "max position notional at directional extreme price".
                target_total_qty = total_buy_notional / target_price
                scale = target_total_qty / total_qty_raw
                per_grid_qty = [qty * scale for qty in raw_qty]
                per_grid_notionals = [
                    per_grid_qty[i] * entry_ref_prices[i] for i in range(n)
                ]
    effective_total_buy_notional = sum(per_grid_notionals)
    if effective_total_buy_notional <= 0:
        raise ValueError("effective_total_buy_notional must be > 0")
    if capital_base_notional <= 0:
        capital_base_notional = effective_total_buy_notional

    open_qty = [0.0] * n
    position_side = [0] * n  # 1=long, -1=short, 0=flat
    entry_price = [0.0] * n

    realized_pnl = 0.0
    total_fees = 0.0
    funding_pnl = 0.0
    funding_event_count = 0
    gross_trade_notional = 0.0
    wins = 0
    closed_trades = 0
    fill_count = 0
    traded_notional = 0.0
    capital_usage_series: list[float] = []
    equity_series: list[float] = []
    trades: list[Trade] | None = [] if capture_trades else None
    funding_events: list[FundingEvent] | None = [] if capture_funding_events else None
    funding_items = sorted(funding_rates, key=lambda x: x.ts) if funding_rates else []
    funding_idx = 0
    prev_boundary = candles[0].open_time
    first_fill_time = None
    first_fill_price = None
    market_start_price = candles[0].open
    market_start_time = candles[0].open_time

    def _mark_first_fill(ts, fill_price: float) -> None:
        nonlocal first_fill_time, first_fill_price
        if first_fill_time is None:
            first_fill_time = ts
            first_fill_price = fill_price

    def _record_open_long(i: int, fill_price: float, ts) -> None:
        nonlocal gross_trade_notional, total_fees, fill_count
        if position_side[i] != 0:
            return
        qty = per_grid_qty[i]
        notional = fill_price * qty
        fee = notional * fee_rate

        open_qty[i] = qty
        entry_price[i] = fill_price
        position_side[i] = 1
        gross_trade_notional += notional
        total_fees += fee
        fill_count += 1
        _mark_first_fill(ts, fill_price)

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

    def _record_open_short(i: int, fill_price: float, ts) -> None:
        nonlocal gross_trade_notional, total_fees, fill_count
        if position_side[i] != 0:
            return
        qty = per_grid_qty[i]
        notional = fill_price * qty
        fee = notional * fee_rate

        open_qty[i] = qty
        entry_price[i] = fill_price
        position_side[i] = -1
        gross_trade_notional += notional
        total_fees += fee
        fill_count += 1
        _mark_first_fill(ts, fill_price)

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

    def _record_close(i: int, fill_price: float, ts) -> None:
        nonlocal gross_trade_notional, realized_pnl, total_fees, closed_trades, wins, fill_count
        if position_side[i] == 0 or open_qty[i] <= 0.0:
            return
        qty = open_qty[i]
        notional = fill_price * qty
        fee = notional * fee_rate
        if position_side[i] > 0:
            pnl = (fill_price - entry_price[i]) * qty
            close_side = "SELL"
        else:
            pnl = (entry_price[i] - fill_price) * qty
            close_side = "BUY"

        gross_trade_notional += notional
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
                    side=close_side,
                    grid_index=i,
                    price=fill_price,
                    qty=qty,
                    notional=notional,
                    fee=fee,
                )
            )

        open_qty[i] = 0.0
        entry_price[i] = 0.0
        position_side[i] = 0

    def _bootstrap_initial_positions() -> None:
        # Emulate practical grid startup: pre-build inventory for levels above/below
        # start price so the first trend leg can be traded immediately.
        if not bootstrap_positions:
            return
        for i in range(n):
            side = grid_sides[i]
            if side == "long":
                if levels[i + 1] > market_start_price:
                    _record_open_long(
                        i=i,
                        fill_price=market_start_price * (1.0 + slippage),
                        ts=market_start_time,
                    )
            elif side == "short":
                if levels[i] < market_start_price:
                    _record_open_short(
                        i=i,
                        fill_price=market_start_price * (1.0 - slippage),
                        ts=market_start_time,
                    )

    def _process_segment(start_price: float, end_price: float, ts) -> None:
        if end_price < start_price:
            for i in range(n - 1, -1, -1):
                boundary = levels[i]
                if end_price <= boundary < start_price:
                    if grid_sides[i] == "long" and position_side[i] == 0:
                        _record_open_long(i=i, fill_price=boundary * (1.0 + slippage), ts=ts)
                    elif grid_sides[i] == "short" and position_side[i] < 0:
                        _record_close(i=i, fill_price=boundary * (1.0 + slippage), ts=ts)
        elif end_price > start_price:
            for i in range(n):
                boundary = levels[i + 1]
                if start_price < boundary <= end_price:
                    if grid_sides[i] == "long" and position_side[i] > 0:
                        _record_close(i=i, fill_price=boundary * (1.0 - slippage), ts=ts)
                    elif grid_sides[i] == "short" and position_side[i] == 0:
                        _record_open_short(i=i, fill_price=boundary * (1.0 - slippage), ts=ts)

    _bootstrap_initial_positions()

    for candle in candles:
        path = _intrabar_path(candle)
        for idx in range(len(path) - 1):
            _process_segment(path[idx], path[idx + 1], candle.close_time)

        mark_price = candle.close

        open_notional = 0.0
        mark_notional = 0.0
        unrealized = 0.0
        for i in range(n):
            if open_qty[i] > 0.0:
                open_notional += open_qty[i] * entry_price[i]
                if position_side[i] > 0:
                    mark_notional += open_qty[i] * mark_price
                    unrealized += (mark_price - entry_price[i]) * open_qty[i]
                elif position_side[i] < 0:
                    mark_notional -= open_qty[i] * mark_price
                    unrealized += (entry_price[i] - mark_price) * open_qty[i]

        while funding_idx < len(funding_items) and funding_items[funding_idx].ts <= candle.close_time:
            funding_ts = funding_items[funding_idx].ts
            if funding_ts > prev_boundary:
                # Positive rate: longs pay, shorts receive (and vice versa).
                event_pnl = -(mark_notional * funding_items[funding_idx].rate)
                funding_pnl += event_pnl
                funding_event_count += 1
                if funding_events is not None:
                    net_pnl = realized_pnl + unrealized - total_fees + funding_pnl
                    account_equity = capital_base_notional + net_pnl
                    funding_events.append(
                        FundingEvent(
                            ts=funding_ts,
                            rate=funding_items[funding_idx].rate,
                            mark_price=mark_price,
                            position_notional=mark_notional,
                            pnl=event_pnl,
                            cumulative_pnl=funding_pnl,
                            realized_pnl=realized_pnl,
                            unrealized_pnl=unrealized,
                            total_fees=total_fees,
                            net_pnl=net_pnl,
                            account_equity=account_equity,
                        )
                    )
            funding_idx += 1
        prev_boundary = candle.close_time

        capital_usage_series.append(open_notional)
        equity_series.append(realized_pnl + unrealized - total_fees + funding_pnl)

    final_price = candles[-1].close
    if first_fill_time is not None and first_fill_price is not None:
        start_time = first_fill_time
        start_price = first_fill_price
    else:
        start_time = market_start_time
        start_price = market_start_price
    end_time = candles[-1].close_time
    period_low = min(candle.low for candle in candles)
    period_high = max(candle.high for candle in candles)
    period_amplitude = ((period_high - period_low) / period_low) if period_low > 0 else 0.0
    underlying_return = (final_price / start_price - 1.0) if start_price > 0 else 0.0

    unrealized_end = 0.0
    final_position_qty = 0.0
    for i in range(n):
        if open_qty[i] > 0.0:
            if position_side[i] > 0:
                final_position_qty += open_qty[i]
                unrealized_end += (final_price - entry_price[i]) * open_qty[i]
            elif position_side[i] < 0:
                final_position_qty -= open_qty[i]
                unrealized_end += (entry_price[i] - final_price) * open_qty[i]
    final_position_notional = final_position_qty * final_price

    net_profit = realized_pnl + unrealized_end - total_fees + funding_pnl
    total_return = net_profit / capital_base_notional

    backtest_days = (candles[-1].close_time - candles[0].open_time).total_seconds() / 86_400
    if backtest_days > 0 and total_return > -1.0:
        annualized = (1.0 + total_return) ** (365.0 / backtest_days) - 1.0
    else:
        annualized = -1.0 if total_return <= -1.0 else 0.0

    nav = [capital_base_notional + x for x in equity_series]
    max_drawdown = _max_drawdown(nav)

    if max_drawdown > 0:
        calmar = annualized / max_drawdown
    else:
        calmar = math.inf if annualized > 0 else 0.0

    score = calmar if math.isfinite(calmar) else 1e12
    win_rate = wins / closed_trades if closed_trades > 0 else 0.0
    turnover_multiple = (
        gross_trade_notional / capital_base_notional if capital_base_notional > 0 else 0.0
    )
    avg_capital_usage = (
        mean(capital_usage_series) / capital_base_notional if capital_usage_series else 0.0
    )
    max_capital_usage = (
        max(capital_usage_series) / capital_base_notional if capital_usage_series else 0.0
    )

    return BacktestResult(
        n=n,
        allocation_mode=allocation_mode,
        strategy_direction=normalized_direction,
        neutral_anchor_price=anchor_price,
        grid_levels=levels,
        grid_sides=grid_sides,
        per_grid_notionals=per_grid_notionals,
        per_grid_qty=per_grid_qty,
        capital_base_notional=capital_base_notional,
        gross_trade_notional=gross_trade_notional,
        turnover_multiple=turnover_multiple,
        net_profit=net_profit,
        total_fees=total_fees,
        funding_pnl=funding_pnl,
        funding_event_count=funding_event_count,
        total_return=total_return,
        annualized_return=annualized,
        max_drawdown=max_drawdown,
        calmar=calmar,
        score=score,
        trade_count=fill_count,
        trade_volume=traded_notional,
        win_rate=win_rate,
        avg_capital_usage=avg_capital_usage,
        max_capital_usage=max_capital_usage,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_end,
        final_position_qty=final_position_qty,
        final_position_notional=final_position_notional,
        start_time=start_time,
        end_time=end_time,
        start_price=start_price,
        end_price=final_price,
        underlying_return=underlying_return,
        period_low=period_low,
        period_high=period_high,
        period_amplitude=period_amplitude,
        equity_curve=equity_series if capture_curves else None,
        capital_usage_curve=capital_usage_series if capture_curves else None,
        trades=trades,
        funding_events=funding_events,
    )
